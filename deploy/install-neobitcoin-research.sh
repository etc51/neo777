#!/usr/bin/env bash
set -euo pipefail

SOURCE_DIR="${1:-/home/codex/neo777}"
TARGET_DIR=/opt/neobitcoin-research
RELEASES_DIR=/opt/neobitcoin-research-releases
DATA_DIR=/var/lib/neobitcoin-research
LOG_DIR=/var/log/neobitcoin-research
CONFIG_DIR=/etc/neobitcoin-research
SERVICE=neobitcoin-research.service
SCHEDULE_SERVICE=neobitcoin-research-schedule.service
SCHEDULE_TIMER=neobitcoin-research-schedule.timer
ARCHIVE_SERVICE=neobitcoin-research-archive.service
ARCHIVE_TIMER=neobitcoin-research-archive.timer
USER_NAME=neobitcoin-research

if [[ ! -f "${CONFIG_DIR}/tbank-token.txt" ]]; then
  echo "missing ${CONFIG_DIR}/tbank-token.txt" >&2
  exit 20
fi

if [[ ! -f "${SOURCE_DIR}/pyproject.toml" ]]; then
  echo "invalid source directory: ${SOURCE_DIR}" >&2
  exit 21
fi

id -u "${USER_NAME}" >/dev/null 2>&1 || \
  useradd --system --home "${DATA_DIR}" --shell /usr/sbin/nologin "${USER_NAME}"
install -d -o "${USER_NAME}" -g "${USER_NAME}" -m 0750 \
  "${DATA_DIR}" "${DATA_DIR}/reports" "${LOG_DIR}"
install -d -o root -g "${USER_NAME}" -m 0750 \
  "${DATA_DIR}/server_control" "${DATA_DIR}/session_archives"
install -d -o root -g root -m 0755 "${RELEASES_DIR}"
install -d -o root -g "${USER_NAME}" -m 0750 "${CONFIG_DIR}"
chown root:"${USER_NAME}" "${CONFIG_DIR}/tbank-token.txt"
chmod 0640 "${CONFIG_DIR}/tbank-token.txt"

stage="$(mktemp -d /opt/.neobitcoin-research-stage.XXXXXX)"
cleanup() { rm -rf "${stage}"; }
trap cleanup EXIT

tar -C "${SOURCE_DIR}" \
  --exclude=.git --exclude=.venv --exclude=data --exclude=reports \
  -cf - . | tar -C "${stage}" -xf -
python3 -m venv "${stage}/.venv"
"${stage}/.venv/bin/python" -m pip install --upgrade pip
"${stage}/.venv/bin/python" -m pip install \
  --extra-index-url https://opensource.tbank.ru/api/v4/projects/238/packages/pypi/simple \
  "${stage}[dev]"
(
  cd "${stage}"
  .venv/bin/python -m pytest -q \
    tests/test_neobitcoin_research_*.py tests/test_neobitcoin_server_schedule.py
)

release="${RELEASES_DIR}/$(date -u +%Y%m%dT%H%M%SZ)"
mv "${stage}" "${release}"
trap - EXIT
chown -R root:root "${release}"
chmod 0755 "${release}"
chown -R "${USER_NAME}:${USER_NAME}" "${DATA_DIR}" "${LOG_DIR}"

if [[ ! -f "${CONFIG_DIR}/research.env" ]]; then
  install -o root -g "${USER_NAME}" -m 0640 \
    "${release}/deploy/neobitcoin-research.env.example" \
    "${CONFIG_DIR}/research.env"
fi
for unit in \
  "${SERVICE}" "${SCHEDULE_SERVICE}" "${SCHEDULE_TIMER}" \
  "${ARCHIVE_SERVICE}" "${ARCHIVE_TIMER}"; do
  install -o root -g root -m 0644 \
    "${release}/deploy/${unit}" "/etc/systemd/system/${unit}"
done

systemctl daemon-reload
systemctl disable --now "${SERVICE}" >/dev/null 2>&1 || true
previous_target=""
if [[ -L "${TARGET_DIR}" ]]; then
  previous_target="$(readlink -f "${TARGET_DIR}")"
elif [[ -e "${TARGET_DIR}" ]]; then
  previous_target="${RELEASES_DIR}/legacy-$(date -u +%Y%m%dT%H%M%SZ)"
  mv "${TARGET_DIR}" "${previous_target}"
fi
ln -s "${release}" "${TARGET_DIR}.new"
mv -Tf "${TARGET_DIR}.new" "${TARGET_DIR}"

deployment_ok=true
systemctl enable --now "${SCHEDULE_TIMER}" "${ARCHIVE_TIMER}" || deployment_ok=false
systemctl start "${SCHEDULE_SERVICE}" || deployment_ok=false
systemctl is-active --quiet "${SCHEDULE_TIMER}" || deployment_ok=false
systemctl is-active --quiet "${ARCHIVE_TIMER}" || deployment_ok=false
test -f "${DATA_DIR}/server_control/status.json" || deployment_ok=false

if [[ "${deployment_ok}" != true ]]; then
  if [[ -n "${previous_target}" ]]; then
    ln -s "${previous_target}" "${TARGET_DIR}.rollback"
    mv -Tf "${TARGET_DIR}.rollback" "${TARGET_DIR}"
    systemctl start "${SCHEDULE_SERVICE}" || true
  else
    systemctl stop "${SERVICE}" || true
    rm -f "${TARGET_DIR}"
  fi
  echo "deployment failed; previous release restored" >&2
  exit 30
fi

echo "${SCHEDULE_TIMER}=active"
echo "${ARCHIVE_TIMER}=active"
systemctl is-active "${SERVICE}" || true
echo "release=${release}"
