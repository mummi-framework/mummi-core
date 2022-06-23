# Copyright (c) 2021, Lawrence Livermore National Security, LLC. All rights reserved. LLNL-CODE-827197.
# This work was produced at the Lawrence Livermore National Laboratory (LLNL) under contract no. DE-AC52-07NA27344 (Contract 44) between the U.S. Department of Energy (DOE) and Lawrence Livermore National Security, LLC (LLNS) for the operation of LLNL.  See license for disclaimers, notice of U.S. Government Rights and license terms and conditions.
# ------------------------------------------------------------------------------
import os
import socket
import sys
import time
import getpass
from cryptography.fernet import Fernet

# ------------------------------------------------------------------------------
host =socket.gethostname()
username =getpass.getuser()
path = os.environ.get("MUMMI_ROOT","./")
keyFile = open(os.path.abspath(path) + "/cmdKeyFile","r")
keyLine = keyFile.readline()
keyLine = keyLine.rstrip("\r\n")
key, serverHost, port, serverUserId = keyLine.split(" ")
keyFile.close()
fCoder = Fernet(key)

#sock = socket.create_connection((serverHost, port)) # Create a TCP/IP socket


# ------------------------------------------------------------------------------
def main():
    print("Configuration:\nHost: ", serverHost, "Port: ", port, "serverUserId: ", serverUserId)
    try:
        while 1:
            command = input("command: ") # Send data
            sock = socket.create_connection((serverHost, port)) # Create a TCP/IP socket
            if command.lower() == "exit":
                sock.shutdown(socket.SHUT_RDWR)
                sock.close()
                sys.exit(0) 
            message = key + " " + host + " " + username + " " + command 
            message = fCoder.encrypt(message.encode())
            sock.sendall(message)

            amount_received = 0
            amount_expected =  1
            
            while (1):
                data = sock.recv(256)
                amount_received += len(data)
                sys.stdout.write(data.decode("utf-8"))
                if (len(data) < 256):
                    break
            sock.shutdown(socket.SHUT_RDWR)
            sock.close()
    except KeyboardInterrupt:
        print('Exiting...')
    finally:
        print('closing socket')
        sock.close()


# ------------------------------------------------------------------------------
if __name__ == "__main__":
    main()

# ------------------------------------------------------------------------------

