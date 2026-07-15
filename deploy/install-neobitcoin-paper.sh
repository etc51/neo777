#!/usr/bin/env bash
set -Eeuo pipefail
umask 077

if [[ ${EUID} -ne 0 ]]; then
  echo "installer must run as root" >&2
  exit 20
fi

if [[ $# -ne 1 ]]; then
  echo "usage: $0 /absolute/path/to/source" >&2
  exit 21
fi

SOURCE_DIR=$1
TARGET_DIR=/opt/neobitcoin-paper
RELEASES_DIR=/opt/neobitcoin-paper-releases
DATA_DIR=/var/lib/neobitcoin-paper
CONFIG_DIR=/etc/neobitcoin-paper
ENV_FILE=${CONFIG_DIR}/paper.env
TOKEN_FILE=${CONFIG_DIR}/tbank-token
THREAD_FILE=${CONFIG_DIR}/codex-thread-id
SERVICE=neobitcoin-paper.service
ARCHIVE_SERVICE=neobitcoin-paper-archive.service
ARCHIVE_TIMER=neobitcoin-paper-archive.timer
DELIVERY_SERVICE=neobitcoin-paper-delivery.service
DELIVERY_TIMER=neobitcoin-paper-delivery.timer
USER_NAME=neopaper
ARCHIVE_GROUP=neoarchive
UNITS=(
  "${SERVICE}"
  "${ARCHIVE_SERVICE}"
  "${ARCHIVE_TIMER}"
  "${DELIVERY_SERVICE}"
  "${DELIVERY_TIMER}"
)
RUNTIME_UNITS=("${SERVICE}" "${ARCHIVE_TIMER}" "${DELIVERY_TIMER}")

if [[ ! -d ${SOURCE_DIR} ]]; then
  echo "source directory does not exist" >&2
  exit 22
fi
SOURCE_DIR=$(readlink -f "${SOURCE_DIR}")
if [[ ! -f ${SOURCE_DIR}/pyproject.toml || ! -f ${SOURCE_DIR}/neobitcoin_paper/cli.py ]]; then
  echo "source directory does not contain the paper runtime" >&2
  exit 23
fi

install -d -o root -g root -m 0750 "${CONFIG_DIR}"
for credential in "${TOKEN_FILE}" "${THREAD_FILE}"; do
  if [[ ! -f ${credential} || -L ${credential} || ! -s ${credential} ]]; then
    echo "required root-owned credential file is missing or invalid" >&2
    exit 24
  fi
  chown root:root "${credential}"
  chmod 0600 "${credential}"
done

if find "${SOURCE_DIR}" \
  -path "${SOURCE_DIR}/.git" -prune -o \
  -path "${SOURCE_DIR}/.venv" -prune -o \
  -type f \( -name .env -o -name '*.env.deploy' -o -name tbank-token -o -name codex-thread-id \) \
  -print -quit | grep -q .; then
  echo "refusing to package a source tree containing credential material" >&2
  exit 25
fi

getent group "${USER_NAME}" >/dev/null 2>&1 || groupadd --system "${USER_NAME}"
id -u "${USER_NAME}" >/dev/null 2>&1 || \
  useradd --system --gid "${USER_NAME}" --home-dir "${DATA_DIR}" \
    --shell /usr/sbin/nologin "${USER_NAME}"
getent group "${ARCHIVE_GROUP}" >/dev/null 2>&1 || groupadd --system "${ARCHIVE_GROUP}"
usermod -a -G "${ARCHIVE_GROUP}" "${USER_NAME}"
if id -u codex >/dev/null 2>&1; then
  usermod -a -G "${ARCHIVE_GROUP}" codex
fi
install -d -o root -g root -m 0755 "${RELEASES_DIR}"
install -d -o "${USER_NAME}" -g "${USER_NAME}" -m 0751 "${DATA_DIR}"
install -d -o "${USER_NAME}" -g "${USER_NAME}" -m 0750 \
  "${DATA_DIR}/state" \
  "${DATA_DIR}/active" \
  "${DATA_DIR}/parquet" \
  "${DATA_DIR}/event_windows" \
  "${DATA_DIR}/delivery_outbox" \
  "${DATA_DIR}/delivered" \
  "${DATA_DIR}/quarantine" \
  "${DATA_DIR}/reports" \
  "${DATA_DIR}/logs"
install -d -o "${USER_NAME}" -g "${ARCHIVE_GROUP}" -m 2750 \
  "${DATA_DIR}/daily_archives"

stage=$(mktemp -d "${RELEASES_DIR}/.stage.XXXXXX")
cleanup_stage() {
  if [[ -n ${stage:-} && -d ${stage} && ${stage} == "${RELEASES_DIR}"/.stage.* ]]; then
    rm -rf -- "${stage}"
  fi
}
trap cleanup_stage EXIT

tar -C "${SOURCE_DIR}" \
  --exclude=./.git \
  --exclude=./.venv \
  --exclude=./.pytest_cache \
  --exclude=./.mypy_cache \
  --exclude=./.ruff_cache \
  --exclude=./data \
  --exclude=./reports \
  --exclude=./outputs \
  --exclude=./archives \
  --exclude='*.tar.gz' \
  -cf - . | tar -C "${stage}" -xf -

python3 -m venv "${stage}/.venv"
"${stage}/.venv/bin/python" -m pip install --upgrade pip wheel
"${stage}/.venv/bin/python" -m pip install \
  --extra-index-url https://opensource.tbank.ru/api/v4/projects/238/packages/pypi/simple \
  "${stage}[dev]"
(
  cd "${stage}"
  PAPER_ONLY=true .venv/bin/python -m compileall -q neobitcoin_paper
  PAPER_ONLY=true .venv/bin/python -m pytest -q tests/test_neobitcoin_paper_*.py
  PAPER_ONLY=true .venv/bin/python -m neobitcoin_paper.cli --help >/dev/null
)

release=${RELEASES_DIR}/$(date -u +%Y%m%dT%H%M%SZ)
if [[ -e ${release} ]]; then
  release=${release}-$$
fi
mv "${stage}" "${release}"
stage=
trap - EXIT
chown -R root:root "${release}"
chmod -R go-w "${release}"
chmod 0755 "${release}"

if [[ ! -f ${ENV_FILE} ]]; then
  install -o root -g "${USER_NAME}" -m 0640 \
    "${release}/deploy/neobitcoin-paper.env.example" "${ENV_FILE}"
fi
if [[ $(grep -Ec '^[[:space:]]*PAPER_ONLY=' "${ENV_FILE}") -ne 1 ]] || \
   [[ $(grep -Ec '^[[:space:]]*PAPER_ONLY=true[[:space:]]*$' "${ENV_FILE}") -ne 1 ]] || \
   [[ $(grep -Ec '^[[:space:]]*LIVE_TRADING_ENABLED=false[[:space:]]*$' "${ENV_FILE}") -ne 1 ]] || \
   [[ $(grep -Ec '^[[:space:]]*REAL_ORDERS_ENABLED=false[[:space:]]*$' "${ENV_FILE}") -ne 1 ]] || \
   grep -Eq '^[[:space:]]*(NEOBITCOIN_PAPER_TOKEN_FILE|NEOBITCOIN_PAPER_THREAD_ID_FILE)=' "${ENV_FILE}"; then
  echo "paper.env violates the immutable paper-only or credential boundary" >&2
  exit 26
fi

declare -A prior_enabled=()
declare -A prior_active=()
for unit in "${RUNTIME_UNITS[@]}"; do
  if systemctl is-enabled --quiet "${unit}" 2>/dev/null; then
    prior_enabled["${unit}"]=yes
  else
    prior_enabled["${unit}"]=no
  fi
  if systemctl is-active --quiet "${unit}" 2>/dev/null; then
    prior_active["${unit}"]=yes
  else
    prior_active["${unit}"]=no
  fi
done

previous_target=
target_was_directory=false
if [[ -L ${TARGET_DIR} ]]; then
  previous_target=$(readlink -f "${TARGET_DIR}")
elif [[ -e ${TARGET_DIR} ]]; then
  target_was_directory=true
  previous_target=${RELEASES_DIR}/legacy-$(date -u +%Y%m%dT%H%M%SZ)
fi

restore_previous_release() {
  systemctl stop "${DELIVERY_TIMER}" "${ARCHIVE_TIMER}" "${SERVICE}" \
    "${DELIVERY_SERVICE}" "${ARCHIVE_SERVICE}" >/dev/null 2>&1 || true

  restore_root=
  if [[ -n ${previous_target} && -d ${previous_target} ]]; then
    ln -sfn "${previous_target}" "${TARGET_DIR}.rollback"
    mv -Tf "${TARGET_DIR}.rollback" "${TARGET_DIR}"
    restore_root=${previous_target}
  elif [[ ${target_was_directory} == true && -d ${TARGET_DIR} && ! -L ${TARGET_DIR} ]]; then
    restore_root=${TARGET_DIR}
  fi

  if [[ -n ${restore_root} ]]; then
    for unit in "${UNITS[@]}"; do
      if [[ -f ${restore_root}/deploy/${unit} ]]; then
        install -o root -g root -m 0644 \
          "${restore_root}/deploy/${unit}" "/etc/systemd/system/${unit}"
      else
        rm -f "/etc/systemd/system/${unit}"
      fi
    done
  else
    rm -f "${TARGET_DIR}"
    for unit in "${UNITS[@]}"; do
      rm -f "/etc/systemd/system/${unit}"
    done
  fi

  systemctl daemon-reload
  for unit in "${RUNTIME_UNITS[@]}"; do
    if [[ ${prior_enabled[${unit}]} == yes ]]; then
      systemctl enable "${unit}" >/dev/null 2>&1 || true
    else
      systemctl disable "${unit}" >/dev/null 2>&1 || true
    fi
    if [[ ${prior_active[${unit}]} == yes ]]; then
      systemctl start "${unit}" >/dev/null 2>&1 || true
    fi
  done
  echo "deployment failed; previous paper release restored" >&2
}

rollback_armed=true
handle_deploy_error() {
  local status=$?
  trap - ERR
  set +e
  if [[ ${rollback_armed} == true ]]; then
    restore_previous_release
  fi
  exit "${status}"
}
trap handle_deploy_error ERR

systemctl stop "${DELIVERY_TIMER}" "${ARCHIVE_TIMER}" "${SERVICE}" \
  "${DELIVERY_SERVICE}" "${ARCHIVE_SERVICE}" >/dev/null 2>&1 || true
if [[ -n ${previous_target} && ! -L ${TARGET_DIR} && -e ${TARGET_DIR} ]]; then
  mv "${TARGET_DIR}" "${previous_target}"
fi
for unit in "${UNITS[@]}"; do
  install -o root -g root -m 0644 \
    "${release}/deploy/${unit}" "/etc/systemd/system/${unit}"
done
systemctl daemon-reload
ln -sfn "${release}" "${TARGET_DIR}.new"
mv -Tf "${TARGET_DIR}.new" "${TARGET_DIR}"

deployment_ok=true
if ! /usr/bin/env -i \
     PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin \
     TZ=Europe/Moscow \
     PAPER_ONLY=true \
     LIVE_TRADING_ENABLED=false \
     REAL_ORDERS_ENABLED=false \
     NEOBITCOIN_PAPER_DATA="${DATA_DIR}" \
     NEOBITCOIN_PAPER_TOKEN_FILE=/dev/null \
     NEOBITCOIN_PAPER_THREAD_ID_FILE=/dev/null \
     "${release}/.venv/bin/python" -m neobitcoin_paper.cli migrate --confirm PAPER_ONLY; then
  deployment_ok=false
fi
if [[ ${deployment_ok} == true ]]; then
  if ! systemctl enable "${SERVICE}" "${ARCHIVE_TIMER}" "${DELIVERY_TIMER}"; then
    deployment_ok=false
  fi
fi
if [[ ${deployment_ok} == true ]]; then
  if ! systemctl restart "${SERVICE}" "${ARCHIVE_TIMER}" "${DELIVERY_TIMER}"; then
    deployment_ok=false
  fi
fi
if [[ ${deployment_ok} == true ]]; then
  for unit in "${SERVICE}" "${ARCHIVE_TIMER}" "${DELIVERY_TIMER}"; do
    if ! systemctl is-active --quiet "${unit}"; then
      deployment_ok=false
    fi
  done
fi

health_ok=false
if [[ ${deployment_ok} == true ]]; then
  deadline=$((SECONDS + 120))
  while [[ ${SECONDS} -lt ${deadline} ]]; do
    if "${release}/.venv/bin/python" -c \
      'import urllib.request; urllib.request.urlopen("http://127.0.0.1:8787/healthz", timeout=3).read()' \
      >/dev/null 2>&1; then
      health_ok=true
      break
    fi
    sleep 2
  done
fi
if [[ ${health_ok} != true ]]; then
  deployment_ok=false
fi

if [[ ${deployment_ok} != true ]]; then
  rollback_armed=false
  trap - ERR
  restore_previous_release
  exit 30
fi

rollback_armed=false
trap - ERR
echo "${SERVICE}=active"
echo "${ARCHIVE_TIMER}=active"
echo "${DELIVERY_TIMER}=active"
echo "health=http://127.0.0.1:8787/healthz"
echo "release=${release}"
