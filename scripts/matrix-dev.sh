#!/usr/bin/env bash
# Starts a throwaway Matrix homeserver on 127.0.0.1:${PORT:-8008} (the image
# the e2e test pins) and registers @relay (the bot) and @alice:
#   scripts/matrix-dev.sh          then: docker rm -f agent-relay-matrix
# Log in as alice with a client such as gomuks or Element, homeserver
# http://127.0.0.1:8008, and start a DM with @relay:localhost, encryption off.
set -euo pipefail
image=$(sed -n 's/^const homeserverImage = "\(.*\)"$/\1/p' "$(dirname "$0")/../cmd/agent-relay/matrix_e2e_test.go")
hs=http://127.0.0.1:${PORT:-8008}
docker run -d --rm --name agent-relay-matrix -p "127.0.0.1:${PORT:-8008}:8008" \
  -e CONTINUWUITY_SERVER_NAME=localhost -e CONTINUWUITY_DATABASE_PATH=/tmp/db \
  -e CONTINUWUITY_ADDRESS=0.0.0.0 -e CONTINUWUITY_PORT=8008 \
  -e CONTINUWUITY_ALLOW_REGISTRATION=true -e CONTINUWUITY_REGISTRATION_TOKEN=dev "$image" >/dev/null
until first=$(docker logs agent-relay-matrix 2>&1 | sed 's/\x1b\[[0-9;]*m//g' | grep -o 'registration token [A-Za-z0-9]*' | head -1 | cut -d' ' -f3) &&
  [ -n "$first" ] && curl -sf "$hs/_matrix/client/versions" >/dev/null; do sleep 0.2; done
register() { # name password token
  session=$(curl -s -XPOST "$hs/_matrix/client/v3/register" -d "{\"username\":\"$1\"}" | sed 's/.*"session":"\([^"]*\)".*/\1/')
  curl -sf -XPOST "$hs/_matrix/client/v3/register" \
    -d "{\"username\":\"$1\",\"password\":\"$2\",\"auth\":{\"type\":\"m.login.registration_token\",\"token\":\"$3\",\"session\":\"$session\"}}"
}
register admin admin-password "$first" >/dev/null
bot=$(register relay relay-password dev)
register alice alice-password dev >/dev/null
token=$(sed 's/.*"access_token":"\([^"]*\)".*/\1/' <<<"$bot")
cat <<END
homeserver:    $hs
bot:           @relay:localhost  access token $token
user:          @alice:localhost  password alice-password
setup:         echo '{"homeserver":"$hs","user_id":"@relay:localhost","access_token":"$token","agent":["invokr","acp"]}' | agent-relay setup --apply matrix
               agent-relay setup --pair matrix
stop:          docker rm -f agent-relay-matrix
END
