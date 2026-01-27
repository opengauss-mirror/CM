dnf install -y nlohmann-json nlohmann-json-devel

Usage
(1) Compile the project
Execute in the project root directory:

bash
make

After compilation is complete, the dist directory will be created automatically, containing the following files:

atf: Server executable file
atf_client_test: Client executable file

(2) Start the server
bash
./dist/atf start

[root@lab1 atf_test]# sh atf_start.sh

Logs will be written to dist/atf_server.log
PID file will be written to dist/atf.pid

(3) Check server status
bash
./dist/atf status

[root@lab1 atf_test]# systemctl status atf.service

(4) Stop the server
bash
./dist/atf stop

[root@lab1 atf_test]# sh atf_stop.sh


(5) Run the client
bash
./dist/atf_client_test

(6) Clean compilation products
bash
# Clean executable files, logs, PID files (retain the dist directory)
make clean

# Completely delete the dist directory
make distclean


systemctl start atf.service	Start the service
systemctl stop atf.service	Stop the service
systemctl restart atf.service	Restart the service (required after modifying the configuration)
systemctl enable atf.service	Set the service to start automatically on boot
systemctl disable atf.service	Disable automatic startup of the service on boot

Import system trusted certificate
keytool -import -alias atf-server \
        -file /data/atf_test/ssl/server.pem \
        -keystore /usr/lib/jvm/java-1.8.0-openjdk-1.8.0.432.b06-0.oe2203sp4.x86_64/jre/lib/security/cacerts


Compile all programs: make
Run the client directly: make run-client
Debug the server with Valgrind: make valgrind-server
Debug the client with Valgrind: make valgrind-client
Clean products: make clean or make distclean