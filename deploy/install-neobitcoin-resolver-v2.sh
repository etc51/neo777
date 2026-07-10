#!/usr/bin/env bash
set -euo pipefail

if [[ ${EUID} -ne 0 ]]; then
  echo "installer must run as root" >&2
  exit 1
fi

SOURCE_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
TARGET_ROOT=/opt/neobitcoin-resolver
DATA_ROOT=/var/lib/neobitcoin-resolver
ENV_DIR=/etc/neobitcoin-resolver
ENV_FILE=${ENV_DIR}/resolver.env
LEGACY_ENV=/etc/neo-trader/neobitcoin-resolver.env
STAMP=$(date -u +%Y%m%dT%H%M%SZ)
STAGE_ROOT=/opt/.neobitcoin-resolver-stage-${STAMP}

if ! id neo-trader >/dev/null 2>&1; then
  useradd --system --home-dir "${DATA_ROOT}" --shell /usr/sbin/nologin neo-trader
fi

install -d -m 0750 "${ENV_DIR}"
if [[ ! -f ${ENV_FILE} ]]; then
  if [[ ! -f ${LEGACY_ENV} ]]; then
    echo "resolver token env is missing" >&2
    exit 1
  fi
  install -m 0600 "${LEGACY_ENV}" "${ENV_FILE}"
fi

set_env() {
  local key=$1
  local value=$2
  if grep -q "^${key}=" "${ENV_FILE}"; then
    sed -i "s|^${key}=.*|${key}=${value}|" "${ENV_FILE}"
  else
    printf '%s=%s\n' "${key}" "${value}" >>"${ENV_FILE}"
  fi
}

if ! grep -Eq '^NEO_TRADER_TBANK_TOKEN=.{10,}$' "${ENV_FILE}"; then
  if grep -Eq '^TBANK_TOKEN=.{10,}$' "${ENV_FILE}"; then
    token_value=$(grep -E '^TBANK_TOKEN=.{10,}$' "${ENV_FILE}" | tail -n 1 | cut -d= -f2-)
    printf 'NEO_TRADER_TBANK_TOKEN=%s\n' "${token_value}" >>"${ENV_FILE}"
    unset token_value
  else
    echo "T-Bank token is absent from resolver env" >&2
    exit 1
  fi
fi

set_env PAPER_MODE true
set_env MULTI_PAIR_MODE false
set_env LIVE_TRADING false
set_env TRADING_MODE readonly
set_env NEO_TRADER_TRADING_MODE readonly
set_env LIVE_TRADING_ENABLED false
set_env NEO_TRADER_LIVE_TRADING_ENABLED false
set_env NEOBITCOIN_DATA_DIR "${DATA_ROOT}"
set_env NEOBITCOIN_DB_PATH "${DATA_ROOT}/neobitcoin_resolver.sqlite"
set_env NEOBITCOIN_DASHBOARD_PATH \
  "${DATA_ROOT}/monitoring/neobitcoin_resolver_dashboard_state.json"
set_env NEOBITCOIN_HEARTBEAT_PATH \
  "${DATA_ROOT}/monitoring/neobitcoin_resolver_heartbeat.txt"
set_env NEOBITCOIN_REPORTS_DIR "${DATA_ROOT}/reports"
set_env NEOBITCOIN_POLL_INTERVAL_SECONDS 5
chmod 0600 "${ENV_FILE}"

install -d -o neo-trader -g neo-trader -m 0750 \
  "${DATA_ROOT}" "${DATA_ROOT}/monitoring" "${DATA_ROOT}/reports" "${DATA_ROOT}/logs"

install -d -m 0750 "${STAGE_ROOT}"
tar --exclude=.git --exclude=.venv --exclude=.pytest_cache \
  --exclude=data --exclude=reports -C "${SOURCE_ROOT}" -cf - . | \
  tar -C "${STAGE_ROOT}" -xf -
python3 -m venv "${STAGE_ROOT}/.venv"
"${STAGE_ROOT}/.venv/bin/python" -m pip install \
  --disable-pip-version-check --quiet "${STAGE_ROOT}"

SMOKE_ROOT=/tmp/neobitcoin-resolver-smoke-${STAMP}
install -d -o neo-trader -g neo-trader -m 0750 "${SMOKE_ROOT}"
runuser -u neo-trader -- env \
  PAPER_MODE=true \
  MULTI_PAIR_MODE=false \
  LIVE_TRADING=false \
  NEOBITCOIN_DATA_DIR="${SMOKE_ROOT}" \
  NEOBITCOIN_DB_PATH="${SMOKE_ROOT}/resolver.sqlite" \
  NEOBITCOIN_DASHBOARD_PATH="${SMOKE_ROOT}/state.json" \
  NEOBITCOIN_HEARTBEAT_PATH="${SMOKE_ROOT}/heartbeat.txt" \
  "${STAGE_ROOT}/.venv/bin/python" \
  "${STAGE_ROOT}/scripts/run_neobitcoin_resolver.py" \
  --mock-data --db "${SMOKE_ROOT}/resolver.sqlite" >/dev/null

systemctl stop neobitcoin-resolver-dashboard.service neobitcoin-resolver.service || true
if [[ -e ${TARGET_ROOT} ]]; then
  mv "${TARGET_ROOT}" "${TARGET_ROOT}.backup-${STAMP}"
fi
mv "${STAGE_ROOT}" "${TARGET_ROOT}"
chown -R root:neo-trader "${TARGET_ROOT}"
chmod -R g+rX "${TARGET_ROOT}"

install -m 0644 "${TARGET_ROOT}/deploy/neobitcoin-resolver.service" \
  /etc/systemd/system/neobitcoin-resolver.service
install -m 0644 "${TARGET_ROOT}/deploy/neobitcoin-resolver-dashboard.service" \
  /etc/systemd/system/neobitcoin-resolver-dashboard.service
systemctl daemon-reload
systemctl enable neobitcoin-resolver.service neobitcoin-resolver-dashboard.service >/dev/null
systemctl restart neobitcoin-resolver.service
sleep 5
systemctl restart neobitcoin-resolver-dashboard.service
sleep 20

systemctl is-active neobitcoin-resolver.service
systemctl is-active neobitcoin-resolver-dashboard.service
curl --silent --show-error --max-time 5 http://127.0.0.1:8036/health || true
