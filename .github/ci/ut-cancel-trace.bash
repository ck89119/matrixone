# Diagnostic-only BASH_ENV: preserve the runner's traps, signals and stdout.
# The two selected fixtures use descriptors 7-9; 19 is a private trace file,
# not a pipe whose reader could keep cancellation from completing.
case "${CASE_DIR:-}" in
    */TestEmbeddedPrebuiltOuterCancellationKillsResistantGroup*/*|*/TestHeavyPlanCancellationStopsWritersBeforeMerge*/*)
        if [[ -n "${MO_CANCEL_TRACE_DIR:-}" ]]; then
            mo_cancel_trace_name=${CASE_DIR%/*}
            mo_cancel_trace_name=${mo_cancel_trace_name##*/}
            exec 19>>"${MO_CANCEL_TRACE_DIR}/${mo_cancel_trace_name}.bash-trace" || exit 125
            BASH_XTRACEFD=19
            PS4='+ time=${EPOCHREALTIME} pid=${BASHPID} line=${LINENO} function=${FUNCNAME[0]:-main}: '
            unset mo_cancel_trace_name
            set -x
        fi
        ;;
esac
