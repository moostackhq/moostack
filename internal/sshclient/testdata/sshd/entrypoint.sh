# The authorized key comes from AUTHORIZED_KEY, for root, op and nosudo.
set -e
for home in /root /home/op /home/nosudo; do
	mkdir -p "$home/.ssh"
	printf '%s\n' "$AUTHORIZED_KEY" > "$home/.ssh/authorized_keys"
	chmod 700 "$home/.ssh"
	chmod 600 "$home/.ssh/authorized_keys"
	chown -R "$(stat -c %U "$home")" "$home/.ssh"
done
sed -i 's/^#\?PermitRootLogin.*/PermitRootLogin prohibit-password/; s/^#\?AllowTcpForwarding.*/AllowTcpForwarding yes/' /etc/ssh/sshd_config
exec /usr/sbin/sshd -D -e
