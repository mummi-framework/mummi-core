# Copyright (c) 2021, Lawrence Livermore National Security, LLC. All rights reserved. LLNL-CODE-827197.
# This work was produced at the Lawrence Livermore National Laboratory (LLNL) under contract no. DE-AC52-07NA27344 (Contract 44) between the U.S. Department of Energy (DOE) and Lawrence Livermore National Security, LLC (LLNS) for the operation of LLNL.  See license for disclaimers, notice of U.S. Government Rights and license terms and conditions.
# ------------------------------------------------------------------------------
import socket
import sys
import os
import time
import platform
import datetime
from subprocess import Popen, PIPE
import getpass
from cryptography.fernet import Fernet
import traceback

# ------------------------------------------------------------------------------
host = socket.gethostname()
serverUserId = getpass.getuser()
key = Fernet.generate_key()
fCoder = Fernet(key)

path = os.path.abspath(os.environ.get("MUMMI_ROOT","./"))
port = int(os.environ.get("MUMMI_DAEMON_PORT", 10000))
keyFileName = os.path.join(path, "cmdKeyFile")
keyLogName = os.path.join(path, "cmdLogFile")

allowed = ["echo", "env", "cat", "flux", "flxuri", "flxls", "sh", "bkill", "bsub", "bjobs", "bresume", "maestro", "ls" ]
FLUX_CMDS = set(["flux", "flxuri", "flxls"])

# ------------------------------------------------------------------------------
def main():

    print('writing file ({})'.format(keyFileName))
    keyFile = open(keyFileName, "w")
    keyFileLine = key.decode() + f" {host} " + str(port) + f" {serverUserId}"
    print('writing file ({})'.format(keyFileLine))
    keyFile.write(keyFileLine+'\n')
    keyFile.close()

    # make the file read-only for the group so no one else can start a server
    os.chmod(keyFileName, 0o640)

    # --------------------------------------------------------------------------
    logFile = open(keyLogName, "a")
    host_address = (host) # Create a TCP/IP socket

    # --------------------------------------------------------------------------
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind((host_address,port)) # Bind the socket to the host
    print('starting up on %s port %s' % (host_address, port))
    try:
        while True:
            try:
                sock.listen()
                print('waiting for a connection...')
                connection, client_address = sock.accept()
                print('client connected: ', client_address)
                while True:
                    print("Waiting for command...")
                    data = connection.recv(1024)
                    if not data:
                        break
                    clientKey, clientHost, user, command = fCoder.decrypt(data).decode("utf-8").split(" ",3)
                    print(command)
                    now = datetime.datetime.now()
                    line = f"{now.strftime('%Y-%m-%d:%H:%M:%S ')} {clientHost} {user} {command}\n"
                    logFile.writelines(line)
                    logFile.flush()
                    cmdList = command.split(" ")

                    retOut = "-----------------------------------------------\n"
                    if cmdList[0] in allowed :
                        if cmdList and cmdList[0] in FLUX_CMDS:
                            cmdList = ["env", "FLUX_URI=`cat $MUMMI_ROOT/flux/flux.info`"] + cmdList

                        cmdList = [os.path.expandvars(c) for c in cmdList]
                        _process = Popen(" ".join(cmdList), stdout=PIPE, stderr=PIPE, shell=True, env=os.environ)
                        out, err = _process.communicate()
                        if _process.returncode != 0:
                            retOut += f"{err.decode('utf-8')}"
                        else:
                            retOut += f"{out.decode('utf-8')}"
                        retOut += \
                            "-----------------------------------------------\n" \
                            f"RETURN CODE: {_process.returncode}\n"
                    else:
                        retOut += f"COMMAND: <{cmdList[0] }> is not allowed\n"
                    retOut += "-----------------------------------------------\n"
                    connection.sendall(retOut.encode())
                connection.close()
            except OSError as os_err:
                # Bind the socket to the host
                print ('Socket error encountered! -- Attempting to recreate.')
                traceback.print_exc()

    except KeyboardInterrupt:
        print("Exiting...")
    finally:
        logFile.close()
        sock.shutdown(socket.SHUT_RDWR)
        sock.close()


# ------------------------------------------------------------------------------
if __name__ == "__main__":
    main()

# ------------------------------------------------------------------------------
