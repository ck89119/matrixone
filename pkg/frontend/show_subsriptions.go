// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import (
	"context"
	"fmt"
	"go/constant"
	"regexp"
	"slices"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

const (
	getAccountIdNamesSql = "select account_id, account_name from mo_catalog.mo_account where status != 'suspend'"
	getPubsSql           = "select pub_name, database_name, account_list, created_time from mo_catalog.mo_pubs"
	getSubsFormat        = "select datname, dat_createsql, created_time from mo_catalog.mo_database where dat_type = 'subscription' and account_id = %d"
)

var (
	showSubscriptionOutputColumns = [6]Column{
		&MysqlColumn{
			ColumnImpl: ColumnImpl{
				name:       "pub_name",
				columnType: defines.MYSQL_TYPE_VARCHAR,
			},
		},
		&MysqlColumn{
			ColumnImpl: ColumnImpl{
				name:       "pub_account",
				columnType: defines.MYSQL_TYPE_VARCHAR,
			},
		},
		&MysqlColumn{
			ColumnImpl: ColumnImpl{
				name:       "pub_database",
				columnType: defines.MYSQL_TYPE_VARCHAR,
			},
		},
		&MysqlColumn{
			ColumnImpl: ColumnImpl{
				name:       "pub_time",
				columnType: defines.MYSQL_TYPE_TIMESTAMP,
			},
		},
		&MysqlColumn{
			ColumnImpl: ColumnImpl{
				name:       "sub_name",
				columnType: defines.MYSQL_TYPE_VARCHAR,
			},
		},
		&MysqlColumn{
			ColumnImpl: ColumnImpl{
				name:       "sub_time",
				columnType: defines.MYSQL_TYPE_TIMESTAMP,
			},
		},
	}
)

type publication struct {
	pubName     string
	pubAccount  string
	pubDatabase string
	pubTime     string
}

type subscription struct {
	publication

	subName string
	subTime string
}

func getAccountIdByName(ctx context.Context, ses *Session, bh BackgroundExec, name string) int32 {
	if accountIds, _, err := getAccountIdNames(ctx, ses, bh, name); err == nil && len(accountIds) > 0 {
		return accountIds[0]
	}
	return -1
}

func getAccountIdNames(ctx context.Context, ses *Session, bh BackgroundExec, likeName string) ([]int32, []string, error) {
	bh.ClearExecResultBatches()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, uint32(sysAccountID))
	sql := getAccountIdNamesSql
	if len(likeName) > 0 {
		sql += fmt.Sprintf(" and account_name like '%s'", likeName)
	}
	if err := bh.Exec(ctx, sql); err != nil {
		return nil, nil, err
	}

	var accountIds []int32
	var accountNames []string
	for _, batch := range bh.GetExecResultBatches() {
		mrs := &MysqlResultSet{
			Columns: make([]Column, len(batch.Vecs)),
		}
		oq := newFakeOutputQueue(mrs)
		for i := 0; i < batch.RowCount(); i++ {
			row, err := extractRowFromEveryVector(ctx, ses, batch, i, oq, true)
			if err != nil {
				return nil, nil, err
			}

			// column[0]: account_id
			accountIds = append(accountIds, row[0].(int32))
			// column[1]: account_name
			accountNames = append(accountNames, string(row[1].([]byte)[:]))
		}
	}
	return accountIds, accountNames, nil
}

func canSub(subAccount, subAccountListStr string) bool {
	if strings.ToLower(subAccountListStr) == "all" {
		return true
	}

	for _, acc := range strings.Split(subAccountListStr, ",") {
		if acc == subAccount {
			return true
		}
	}
	return false
}

func getPubs(ctx context.Context, ses *Session, bh BackgroundExec, accountId int32, accountName string, like string, subAccountName string) ([]*publication, error) {
	bh.ClearExecResultBatches()
	sql := getPubsSql
	if len(like) > 0 {
		sql += fmt.Sprintf(" where pub_name like '%s';", like)
	}
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, uint32(accountId))
	if err := bh.Exec(ctx, sql); err != nil {
		return nil, err
	}

	var pubs []*publication
	for _, batch := range bh.GetExecResultBatches() {
		mrs := &MysqlResultSet{
			Columns: make([]Column, len(batch.Vecs)),
		}
		oq := newFakeOutputQueue(mrs)
		for i := 0; i < batch.RowCount(); i++ {
			row, err := extractRowFromEveryVector(ctx, ses, batch, i, oq, true)
			if err != nil {
				return nil, err
			}

			pubName := string(row[0].([]byte)[:])
			pubDatabase := string(row[1].([]byte)[:])
			subAccountListStr := string(row[2].([]byte)[:])
			pubTime := row[3].(string)
			if !canSub(subAccountName, subAccountListStr) {
				continue
			}

			pub := &publication{
				pubName:     pubName,
				pubAccount:  accountName,
				pubDatabase: pubDatabase,
				pubTime:     pubTime,
			}
			pubs = append(pubs, pub)
		}
	}
	return pubs, nil
}

func getSubInfoFromSql(ctx context.Context, ses FeSession, sql string) (subName, pubAccountName, pubName string, err error) {
	var lowerAny interface{}
	if lowerAny, err = ses.GetGlobalVar(ctx, "lower_case_table_names"); err != nil {
		return
	}

	var ast []tree.Statement
	if ast, err = mysql.Parse(ctx, sql, lowerAny.(int64), 0); err != nil {
		return
	}
	defer func() {
		for _, s := range ast {
			s.Free()
		}
	}()

	subName = string(ast[0].(*tree.CreateDatabase).Name)
	pubAccountName = string(ast[0].(*tree.CreateDatabase).SubscriptionOption.From)
	pubName = string(ast[0].(*tree.CreateDatabase).SubscriptionOption.Publication)
	return
}

func getSubs(ctx context.Context, ses *Session, bh BackgroundExec, accountId uint32, like string) ([]*subscription, error) {
	bh.ClearExecResultBatches()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)
	sql := fmt.Sprintf(getSubsFormat, accountId)
	if err := bh.Exec(ctx, sql); err != nil {
		return nil, err
	}

	var subs []*subscription
	for _, batch := range bh.GetExecResultBatches() {
		mrs := &MysqlResultSet{
			Columns: make([]Column, len(batch.Vecs)),
		}
		oq := newFakeOutputQueue(mrs)
		for i := 0; i < batch.RowCount(); i++ {
			row, err := extractRowFromEveryVector(ctx, ses, batch, i, oq, true)
			if err != nil {
				return nil, err
			}

			subName := string(row[0].([]byte)[:])
			createSql := string(row[1].([]byte)[:])
			subTime := row[2].(string)
			_, pubAccountName, pubName, err := getSubInfoFromSql(ctx, ses, createSql)
			if err != nil {
				return nil, err
			}

			if len(like) > 0 && !isLike(pubName, like) {
				continue
			}

			sub := &subscription{
				publication: publication{
					pubName:    pubName,
					pubAccount: pubAccountName,
				},
				subName: subName,
				subTime: subTime,
			}
			subs = append(subs, sub)
		}
	}
	return subs, nil
}

func doShowSubscriptions(ctx context.Context, ses *Session, ss *tree.ShowSubscriptions) (err error) {
	bh := GetRawBatchBackgroundExec(ctx, ses)
	defer bh.Close()

	if err = bh.Exec(ctx, "begin;"); err != nil {
		return err
	}
	defer func() {
		err = finishTxn(ctx, bh, err)
	}()

	var like string
	if ss.Like != nil {
		right, ok := ss.Like.Right.(*tree.NumVal)
		if !ok || right.Value.Kind() != constant.String {
			return moerr.NewInternalError(ctx, "like clause must be a string")
		}
		like = constant.StringVal(right.Value)
	}

	// step 1. get all account
	var accountIds []int32
	var accountNames []string
	if accountIds, accountNames, err = getAccountIdNames(ctx, ses, bh, ""); err != nil {
		return err
	}

	// step 2. traversal all accounts, get all publication pubs
	var allPubs []*publication
	for i := 0; i < len(accountIds); i++ {
		var pubs []*publication
		if pubs, err = getPubs(ctx, ses, bh, accountIds[i], accountNames[i], like, ses.GetTenantName()); err != nil {
			return err
		}

		allPubs = append(allPubs, pubs...)
	}

	// step 3. get current account's subscriptions
	subs, err := getSubs(ctx, ses, bh, ses.GetTenantInfo().TenantID, like)
	if err != nil {
		return err
	}

	// step 4. combine pubs && subs, and sort
	subscribed := make([]bool, len(allPubs))
	for _, sub := range subs {
		idx := slices.IndexFunc(allPubs, func(p *publication) bool {
			return p.pubAccount == sub.pubAccount && p.pubName == sub.pubName
		})

		if idx != -1 {
			subscribed[idx] = true
			sub.pubDatabase = allPubs[idx].pubDatabase
			sub.pubTime = allPubs[idx].pubTime
		}
	}
	// add unsubscribed pubs if all option set
	if ss.All {
		for i, v := range subscribed {
			if !v {
				subs = append(subs, &subscription{publication: *allPubs[i]})
			}
		}
	}

	if len(like) > 0 {
		// sort by pub_name asc
		sort.SliceStable(subs, func(i, j int) bool {
			return subs[i].pubName < subs[j].pubName
		})
	} else {
		// sort by sub_time, pub_time desc
		sort.SliceStable(subs, func(i, j int) bool {
			if subs[i].subTime != subs[j].subTime {
				return subs[i].subTime > subs[j].subTime
			}
			return subs[i].pubTime > subs[j].pubTime
		})
	}

	// step 5. generate result set
	bh.ClearExecResultBatches()
	var rs = &MysqlResultSet{}
	for _, column := range showSubscriptionOutputColumns {
		rs.AddColumn(column)
	}

	for _, sub := range subs {
		rs.AddRow([]interface{}{sub.pubName, sub.pubAccount, nilIfEmpty(sub.pubDatabase), nilIfEmpty(sub.pubTime), nilIfEmpty(sub.subName), nilIfEmpty(sub.subTime)})
	}
	ses.SetMysqlResultSet(rs)
	return nil
}

func nilIfEmpty(s string) interface{} {
	if len(s) == 0 {
		return nil
	}
	return s
}

func isLike(s, pat string) bool {
	replace := func(s string) string {
		var oldCharacter rune

		r := make([]byte, len(s)*2)
		w := 0
		start := 0
		for len(s) > start {
			character, wid := utf8.DecodeRuneInString(s[start:])
			if oldCharacter == '\\' {
				w += copy(r[w:], s[start:start+wid])
				start += wid
				oldCharacter = 0
				continue
			}
			switch character {
			case '_':
				w += copy(r[w:], []byte{'.'})
			case '%':
				w += copy(r[w:], []byte{'.', '*'})
			case '(':
				w += copy(r[w:], []byte{'\\', '('})
			case ')':
				w += copy(r[w:], []byte{'\\', ')'})
			case '\\':
			default:
				w += copy(r[w:], s[start:start+wid])
			}
			start += wid
			oldCharacter = character
		}
		return string(r[:w])
	}
	convert := func(expr []byte) string {
		return fmt.Sprintf("^(?s:%s)$", replace(util.UnsafeBytesToString(expr)))
	}

	realPat := convert([]byte(pat))
	reg, err := regexp.Compile(realPat)
	if err != nil {
		return false
	}
	return reg.Match([]byte(s))
}
