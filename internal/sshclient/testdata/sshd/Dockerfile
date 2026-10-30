FROM alpine:3.22
RUN apk add --no-cache openssh-server sudo socat \
 && adduser -D op && passwd -u op \
 && echo 'op ALL=(ALL) NOPASSWD: ALL' > /etc/sudoers.d/op \
 && adduser -D nosudo && passwd -u nosudo \
 && ssh-keygen -A
COPY entrypoint.sh /entrypoint.sh
ENTRYPOINT ["/bin/sh", "/entrypoint.sh"]
