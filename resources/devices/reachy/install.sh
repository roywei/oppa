#!/bin/sh
# Put OPPA's body app on a Reachy Mini and start it at every boot.
#   install.sh ROBOT OPPA_URL STATE_DIR
#   install.sh reachy-mini.local http://my-mac.local:8767 ~/.oppa/channels/reachy
# ROBOT is the robot's address; ssh must log in there as ROBOT_USER (pollen
# unless set) without a prompt. OPPA_URL is where the robot reaches the
# adapter on this Mac. STATE_DIR is the adapter's --state folder, where its
# token is.
set -eu
[ $# -eq 3 ] || { sed -n '2,8p' "$0" | cut -c3-; exit 2; }
robot="${ROBOT_USER:-pollen}@$1" oppa=$2 state=$3
here=$(cd "$(dirname "$0")" && pwd)
token=$(cat "$state/token")
tar -C "$here" -cf - pyproject.toml oppa_body | ssh "$robot" 'rm -rf /tmp/oppa_body && mkdir /tmp/oppa_body && tar -C /tmp/oppa_body -xf -'
printf '{"oppa": "%s", "token": "%s"}\n' "$oppa" "$token" | ssh "$robot" 'umask 077 && mkdir -p ~/.config && cat > ~/.config/oppa-body.json'
ssh "$robot" '/venvs/apps_venv/bin/pip install --quiet --no-deps --force-reinstall /tmp/oppa_body \
  && { curl -sS -o /dev/null -X POST localhost:8000/api/apps/stop-current-app; \
  curl -fsS -o /dev/null -X PUT localhost:8000/api/apps/startup-app -H "Content-Type: application/json" -d "{\"startup_app\": \"oppa_body\"}" \
  && curl -fsS -o /dev/null -X POST localhost:8000/api/apps/start-app/oppa_body; }'
echo "oppa_body is installed on $1 and starts with the robot."
