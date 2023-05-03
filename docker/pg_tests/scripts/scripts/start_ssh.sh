apt-get update && \
    apt-get -y install openssh-server

ssh-keygen -b 2048 -t rsa -f ~/.ssh/id_rsa -q -N "" && \
    cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys

cp ~/.ssh/id_rsa /private_key.id_rsa
chmod 777 /private_key.id_rsa

/etc/init.d/ssh start