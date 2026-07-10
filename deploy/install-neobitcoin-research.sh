#!/usr/bin/env bash
set -euo pipefail

SOURCE_DIR="${1:-/home/codex/neo777}"
TARGET_DIR=/opt/neobitcoin-research
RELEASES_DIR=/opt/neobitcoin-research-releases
DATA_DIR=/var/lib/neobitcoin-research
LOG_DIR=/var/log/neobitcoin-research
CONFIG_DIR=/etc/neobitcoin-research
SERVICE=neobitcoin-research.service
USER_NAME=neobitcoin-research

if [[ ! -f "${CONFIG_DIR}/tbank.token" ]]; then
  echo "missing ${CONFIG_DIR}/tbank.token" >&2
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
install -d -o root -g root -m 0755 "${RELEASES_DIR}"
install -d -o root -g "${USER_NAME}" -m 0750 "${CONFIG_DIR}"
chown root:"${USER_NAME}" "${CONFIG_DIR}/tbank.token"
chmod 0640 "${CONFIG_DIR}/tbank.token"

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
"${stage}/.venv/bin/python" -m pytest -q \
  "${stage}"/tests/test_neobitcoin_research_*.py

release="${RELEASES_DIR}/$(date -u +%Y%m%dT%H%M%SZ)"
mv "${stage}" "${release}"
trap - EXIT
chown -R root:root "${release}"
chown -R "${USER_NAME}:${USER_NAME}" "${DATA_DIR}" "${LOG_DIR}"

if [[ ! -f "${CONFIG_DIR}/research.env" ]]; then
  install -o root -g "${USER_NAME}" -m 0640 \
    "${release}/deploy/neobitcoin-research.env.example" \
    "${CONFIG_DIR}/research.env"
fi
install -o root -g root -m 0644 \
  "${release}/deploy/neobitcoin-research.service" \
  "/etc/systemd/system/${SERVICE}"

systemctl daemon-reload
systemctl enable "${SERVICE}"
previous_target=""
if [[ -L "${TARGET_DIR}" ]]; then
  previous_target="$(readlink -f "${TARGET_DIR}")"
elif [[ -e "${TARGET_DIR}" ]]; then
  previous_target="${RELEASES_DIR}/legacy-$(date -u +%Y%m%dT%H%M%SZ)"
  mv "${TARGET_DIR}" "${previous_target}"
fi
ln -s "${release}" "${TARGET_DIR}.new"
mv -Tf "${TARGET_DIR}.new" "${TARGET_DIR}"

if ! systemctl restart "${SERVICE}" || ! systemctl is-active --quiet "${SERVICE}"; then
  if [[ -n "${previous_target}" ]]; then
    ln -s "${previous_target}" "${TARGET_DIR}.rollback"
    mv -Tf "${TARGET_DIR}.rollback" "${TARGET_DIR}"
    systemctl restart "${SERVICE}" || true
  fi
  echo "deployment failed; previous release restored" >&2
  exit 30
fi

sleep 8
systemctl is-active --quiet "${SERVICE}"
test -f "${DATA_DIR}/state.sqlite"
echo "${SERVICE}=active"
echo "release=${release}"
