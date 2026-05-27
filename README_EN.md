- [What Is CM?](#what-is-cm)
- [Uninstalling CM](#uninstalling-cm)
- [Compilation](#compilation)
    - [Overview](#overview)
    - [OS and Software Dependencies](#os-and-software-dependencies)
    - [Downloading CM](#downloading-cm-and-its-dependencies)
    - [Compiling Third-Party Software](#compiling-third-party-software)
    - [Using build.sh to Compile Code](#using-buildsh-to-compile-code)
    - [Using Commands to Compile Code](#using-commands-to-compile-code)
    - [Compiling an Installation Package](#compiling-an-installation-package)
    - [Appendix: Obtaining the DCC Dynamic Library](#appendix-obtaining-the-dcc-dynamic-library)
- [Installation](#installation)
    - [Creating a Profile](#creating-a-profile)
    - [Initializing the Installation Environment](#initializing-the-installation-environment)
    - [Performing Installation](#performing-installation)
- [Quick Start](#quick-start)
- [Documents](#documents)
- [Community](#community)
    - [Governance](#governance)
    - [Communication](#communication)
- [Contribution](#contribution)
- [Release Notes](#release-notes)
- [License](#license)

## What Is CM?

Cluster Manager (CM) is cluster resource management software. It supports customized resource monitoring and provides capabilities such as monitoring of the primary/standby database status, network communication faults, file system faults, and automatic primary/standby switchover upon faults. It also offers various cluster management capabilities, such as starting and stopping clusters, nodes, and instances, querying cluster status, performing primary/standby switchover, and managing logs.

## Compilation

### Overview

CM compilation requires two repositories: `CM` and `binarylibs`.
To compile the latest CM using the full source code, `DCC`, `DCF`, and `CBB` repositories are also required.

- `CM`: main code of CM, which can be obtained from the open-source community.

- `binarylibs`: third-party open-source software. Run code in `openGauss-third_party` to obtain it, or download the compiled software from the open-source community.

- `DCC`: unified configuration management component. It provides capabilities such as configuration storage and primary node selection. For details, see the DCC documentation.

- `DCF`: distributed consistency framework, which provides the log replication capability. For details, see the DCF documentation.

- `CBB`: public repository, which stores common functions of different components. For details, see the CBB documentation.


For details about how to compile third-party libraries and GCC, see [related blogs](https://opengauss.org/en/blogs/?post/xingchen/opengauss_compile/).

For details about DCC-related compilation dependencies, see [Appendix: Obtaining the DCC Dynamic Library](#appendix-obtaining-the-DCC-dynamic-library)

Before compiling CM, check the OS and software dependencies.

CM can be compiled using `build.sh` or commands. `build.sh` can generate an installation package.

### OS and Software Dependencies

CM supports the following OSs:

- CentOS 7.6 (x86)

- openEuler 20.03 LTS (AArch64)

- openEuler 20.03 LTS (x86_64)

- openEuler 22.03 LTS (AArch64)

- openEuler 22.03 LTS (x86_64)

- openEuler 24.03 LTS (AArch64)

- openEuler 24.03 LTS (x86_64)

For details about how to adapt to other OSs, see [related blogs](https://opengauss.org/en/blogs/?post/xingchen/opengauss_compile/).

The following table lists the system software required for compiling CM.

You are advised to use the default installation packages of the following dependencies obtained from the OS installation CD/DVD-ROM or repositories. If any of the following software does not exist, obtain the recommended software version.

The system software dependencies are as follows.

| Software              | Recommended Version   |
| ----------------- | ----------------|
| glibc-devel    | 2.17-111      |
| lsb_release   | 4.1              |
| libaio-devel   | 0.3.109-13  |

### Downloading CM and Its Dependencies

Download CM, DCC, and openGauss-third_party from the open source community.

https://gitee.com/opengauss/CM

Download the compiled `binarylibs` from the following website: Decompress the downloaded file and rename it `binarylibs`.

https://opengauss.obs.cn-south-1.myhuaweicloud.com/5.1.0/binarylibs/gcc10.3/openGauss-third_party_binarylibs_openEuler_arm.tar.gz

For full source compilation, download the DCC and its dependency CBB and DCF repositories. Otherwise, the build will fallback to the pre-built DCC dynamic libraries in the `binarylibs` directory by default.

Now the complete CM code is ready. Store the code in the following directories (`sda` as an example):

- /sda/CM
- /sda/binarylibs
- /sda/openGauss-third_party
- /sda/DCC
- /sda/CBB
- /sda/DCF

### Compiling Third-Party Software

Before compiling CM, compile the open-source and third-party software on which CM depends. Open-source and third-party software is stored in the `openGauss-third_party` code repository and usually needs to be built only once. If the open-source software is updated, you need to rebuild the software.

Alternatively, obtain the compiled open-source software from the `binarylibs` library.

If you want to compile third-party software by yourself, visit the `openGauss-third_party` repository for details. 

The compiled files are saved in the `binarylibs` directory at the same level as `openGauss-third_party`. These files are used during the compilation of `CM`.

### Compiling Code

#### Using build.sh to Compile Code

`build.sh` in openGauss-CM is an important script tool required for compilation. This tool integrates software compilation and packaging functions.

The following table describes the options.

| Option | Default Value                      | Parameter                                  | Description                                             |
| :---- | :--------------------------- | :------------------------------------- | :------------------------------------------------ |
| -h    | Do not use this option.            | -                                      | Displays the help menu.                                       |
| -m    | release                      | [debug &#124; release &#124; memcheck] | Specifies the target version.                                   |
| -3rd  | ${Code directory}/binarylibs | [binarylibs path]                      | Specifies the `binarylibs` path. It must be an absolute path.       |
| -o  | ${Code directory}/output | [output path]                      | Specifies the output path of the final compilation result.       |
| -pkg  | Do not use this option.            | -                                      | Compresses the compiled code into an installation package.                     |
| --gcc | 10.3                         | [7.3; 10.3]                            | Specifies the GCC version.                                     |

> **Note**
>
> - `-m [debug | release | memcheck]` indicates that three target versions can be selected:
>    - `release`: generates a release binary program. During compilation, kernel debugging code is removed by configuring advanced GCC optimization options. This option is typically used in the production environment or performance test environment.
>    - `debug`: generates a debug binary program. Kernel debugging code is added during compilation. This option is typically used in the developer self-test platform.
>    - `memcheck`: generates a memcheck binary program. During compilation, the ASAN function is added based on the debug version to locate memory issues.
> - `-3rd [binarylibs path]` specifies the path to `binarylibs`. By default, `binarylibs` exists in the current code folder. This option does not need to be specified if `binarylibs` is moved to `CM` or a soft link to `binarylibs` is created in `CM`. However, the file can be easily deleted by the `git clean` command.
> - `-o [output path]` indicates the path of `output`. The default path is the `output` file in the current code folder. After the path is specified, **all files in the folder will be deleted during compilation**. Ensure that the specified path is correct.
> - Each option in the script has a default value. There are only a few options and the dependencies are simple, so the script is easy to use. If the required parameter values differ from the defaults, configure them according to your specific needs.

Run the following command to compile CM:

``` shell
[user@linux CM]$ sh build.sh -m [debug | release | memcheck] -3rd [binarylibs path]
```

Example:

```shell
[user@linux CM]$ sh build.sh -3rd /sda/binarylibs      # Compile and install openGauss of the release version.
[user@linux CM]$ sh build.sh -m debug -3rd /sda/binarylibs    # Compile and install openGauss of the debug version.
```

Installation path of the compiled software: `/sda/CM/output`

Path to the compiled binary file: `/sda/CM/output/bin`

#### Using Commands to Compile Code

1. Configure environment variables.

   ```shell
   export BINARYLIBS=________    # Path of the binarylibs file
   export GCC_PATH=$BINARYLIBS/buildtools/gcc10.3/
   export CC=$GCC_PATH/gcc/bin/gcc
   export CXX=$GCC_PATH/gcc/bin/g++
   export LD_LIBRARY_PATH=$GCC_PATH/gcc/lib64:$GCC_PATH/isl/lib:$GCC_PATH/mpc/lib/:$GCC_PATH/mpfr/lib/:$GCC_PATH/gmp/lib/:$LD_LIBRARY_PATH
   export PATH=$GCC_PATH/gcc/bin:$PATH

   ```
  
2. Prepare the required third-party components.
 
 The `common_lib/dcc` folder in the `root` directory of the CM code is used to store the third-party component `DCC` on which the CM depends.
 
 **For details about how to obtain the DCC dynamic library**, see [Appendix: Obtaining the DCC Dynamic Library](#appendix-obtaining-the-DCC-dynamic-library).
 
 The prepared directory structure is as follows:
 
 ```
 [user@linux CM]$ tree common_lib/dcc/
 common_lib/dcc/
├── include
│   └── dcc_interface.h
└── lib
    ├── libdcc.so
    ├── libdcf.so
    └── libgstor.so

2 directories, 4 files
 ```
  
3. Select a version for compilation configuration.
   
 - **cmake:**
 **Create a compilation directory.**
 
    ```
    mkdir dist
    cd dist
    ```
 
   **debug** version:

   ```
   cmake .. -DCMAKE_INSTALL_PREFIX=`pwd`/../output/ -DCMAKE_BUILD_TYPE=Debug
   ```

   **release** version:

   ```
   cmake .. -DCMAKE_INSTALL_PREFIX=`pwd`/../output/ -DCMAKE_BUILD_TYPE=Release
   ```

   **memcheck** version:

   ```
   cmake .. -DCMAKE_INSTALL_PREFIX=`pwd`/../output/ -DENABLE_MEMCHECK=ON
   ```

4. Run the following commands to compile CM:

- **cmake:**
   ```
   [user@linux CM]$ make -sj
   [user@linux CM]$ make install -sj
   ```

- **makefile:**
   **debug** version:

   ```
   [user@linux CM]$ make -sj
   [user@linux CM]$ make install -sj
   ```

   **release** version:

   ```
   [user@linux CM]$ make BUILD_TYPE=Release -sj
   [user@linux CM]$ make BUILD_TYPE=Release install -sj
   ```

   **memcheck** version:

   ```
   [user@linux CM]$ make ENABLE_MEMCHECK=ON -sj
   [user@linux CM]$ make ENABLE_MEMCHECK=ON install -sj
   ```

- Installation path of the compiled software: `CM/output`

- Path to the compiled binary file: `CM/output/bin`

### Compiling an Installation Package

Read [Using build.sh to Compile Code](#using-buildsh-to-compile-code) to learn how to use `build.sh` to compile CM.

Add the `-pkg` option to compile an installation package.

```
[user@linux openGauss-server]$ sh build.sh -m [debug | release | memcheck] -3rd [binarylibs path] -pkg
```

Example:

```
sh build.sh -pkg       # Create an openGauss installation package of the release version. binarylibs or its soft link must exist in the code directory. Otherwise, the compilation will fail.
sh build.sh -m debug -3rd /sdc/binarylibs -pkg           # Create an openGauss installation package of the debug version.
```

- The generated installation package is stored in `./output`.

### Appendix: Obtaining the DCC Dynamic Library

For details about how to compile the DCC, see the DCC README file. Then, copy the compilation result (that is, the files in the `include` and `lib` directories after DCC is successfully compiled) to the `common_lib` folder in the `root` directory of the CM code.

 ```
 [user@linux CM]$ tree common_lib/dcc/
 common_lib/dcc/
├── include
│   └── dcc_interface.h
└── lib
    ├── libdcc.so
    ├── libdcf.so
    └── libgstor.so

2 directories, 4 files
 ```

## Installation

### Creating a Profile

Before installing openGauss with CM, create the `clusterconfig.xml` configuration file. The XML file contains information about the server where openGauss and CM will be deployed, including the installation path, IP address, and port. The file guides the deployment of openGauss and CM. Configure the XML file based on your scenario. For the installation with CM, the only difference from the openGauss installation is that you need to add CM to the configuration file.

The following describes how to create an XML configuration file for installation of openGauss and CM based on the deployment scheme of one primary node and two standby nodes.
The values of the following parameters are for reference only. You can change them as required. Each line of information has a comment.

```
<?xml version="1.0" encoding="UTF-8"?>
<ROOT>
    <!-- Overall information about openGauss -->
    <CLUSTER>
    <!-- Database name -->
        <PARAM name="clusterName" value="dbCluster" />
    <!-- Database node name (host name) -->
        <PARAM name="nodeNames" value="node1,node2,node3" />
    <!-- Node IP addresses corresponding to node names respectively -->
        <PARAM name="backIp1s" value="192.168.0.11,192.168.0.12,192.168.0.13"/>
    <!-- Database installation directory -->
        <PARAM name="gaussdbAppPath" value="/opt/huawei/install/app" />
    <!-- Log directory -->
        <PARAM name="gaussdbLogPath" value="/var/log/omm" />
    <!-- Temporary file directory -->
        <PARAM name="tmpMppdbPath" value="/opt/huawei/tmp"/>
    <!-- Database tool directory -->
        <PARAM name="gaussdbToolPath" value="/opt/huawei/install/om" />
    <!--Directory of the core file of the database -->
        <PARAM name="corePath" value="/opt/huawei/corefile"/>
    <!-- openGauss type. The following example uses a single-node system as an example. single-inst indicates that one primary and multiple standby nodes are deployed in a single-node system. -->
        <PARAM name="clusterType" value="single-inst"/>
    </CLUSTER>
    <!-- Node deployment information on each server -->
    <DEVICELIST>
        <!-- Information about node deployment on node1 -->
        <DEVICE sn="1000001">
        <!-- Host name of node1 -->
            <PARAM name="name" value="node1"/>
        <!-- AZ where node1 is located and AZ priority -->
            <PARAM name="azName" value="AZ1"/>
            <PARAM name="azPriority" value="1"/>
        <!-- If only one NIC is available for the server, set <b>backIP1/<b> and <b>sshIP1</b> to the same IP address. -->
            <PARAM name="backIp1" value="192.168.0.11"/>
            <PARAM name="sshIp1" value="192.168.0.11"/>
            
        <!--CM-->
	    <!-- CM data directory -->
            <PARAM name="cmDir" value="/opt/huawei/install/data/cm" />
            <PARAM name="cmsNum" value="1" />
	    <!-- CM listening port -->
            <PARAM name="cmServerPortBase" value="5000" />
            <PARAM name="cmServerlevel" value="1" />
	    <!-- Names and listening IP addresses of the nodes where all CM instances are located -->
            <PARAM name="cmServerListenIp1" value="192.168.0.11,192.168.0.12,192.168.0.13" />
            <PARAM name="cmServerRelation" value="node1,node2,node3" />
            
	    <!--dbnode-->
	    	<PARAM name="dataNum" value="1"/>
	    <!-- Database node port number -->
	    	<PARAM name="dataPortBase" value="26000"/>
	    <!-- Data directories on the primary and standby database nodes -->
	    	<PARAM name="dataNode1" value="/opt/huawei/install/data/db1,node2,/opt/huawei/install/data/db1,node3,/opt/huawei/install/data/db1"/>
	    <!-- Number of database nodes where the synchronization mode is set -->
	    	<PARAM name="dataNode1_syncNum" value="0"/>
        </DEVICE>

        <!-- Node deployment information on node2. The value of name is the host name. -->
        <DEVICE sn="1000002">
            <PARAM name="name" value="node2"/>
            <PARAM name="azName" value="AZ1"/>
            <PARAM name="azPriority" value="1"/>
            <!-- If only one NIC is available for the server, set <b>backIP1/<b> and <b>sshIP1</b> to the same IP address. -->
            <PARAM name="backIp1" value="192.168.0.12"/>
            <PARAM name="sshIp1" value="192.168.0.12"/>
            <PARAM name="cmDir" value="/opt/huawei/install/data/cm" />
        </DEVICE>

        <!-- Node deployment information on node3. The value of name is the host name. -->
        <DEVICE sn="1000003">
            <PARAM name="name" value="node3"/>
            <PARAM name="azName" value="AZ1"/>
            <PARAM name="azPriority" value="1"/>
            <!-- If only one NIC is available for the server, set <b>backIP1/<b> and <b>sshIP1</b> to the same IP address. -->
            <PARAM name="backIp1" value="192.168.0.13"/>
            <PARAM name="sshIp1" value="192.168.0.13"/>
            <PARAM name="cmDir" value="/opt/huawei/install/data/cm" />
        </DEVICE>
    </DEVICELIST>
</ROOT>
```

### Initializing the Installation Environment

After the openGauss and CM configuration file is created, run `gs_preinstall` to prepare the installation user and environment so that you can perform installation and openGauss management operations with the least privilege, ensuring system security.

`gs_preinstall` automatically prepares the installation environment as follows:

- Automatically sets Linux kernel parameters to improve the server loads. These parameters directly affect the running status of the database system. Adjust them only when necessary.
- Automatically copies the openGauss configuration file and installation package to the same directory on the openGauss host.
- Automatically creates the openGauss installation user and user group if they do not exist.
- Reads the directory information in the openGauss configuration file, create the directory, and grants the directory permissions to the installation user.

**Important notes**

- You must check the upper-layer directory permissions to ensure that the installation user has the read, write, and execute permissions on the installation package and configuration file directory.
- The mapping between each host name and IP address in the XML configuration file must be correct.
- Only the `root` user is authorized to run the `gs_preinstall` command.

**Procedure**

Log in to any host where openGauss and CM are to be installed as user `root` and create a directory for storing the installation package as planned.

   ```
mkdir -p /opt/software/openGauss
chmod 755 -R /opt/software
   ```

   > **Note**
   >
   > - Do not create the directory in the home directory of any openGauss user or its subdirectories because you may lack permissions for such directories.
   > - An openGauss user must have the read and write permissions on the `/opt/software/openGauss` directory.

1. Upload the installation packages `openGauss-x.x.x-openEULER-64bit.tar.gz` and `openGauss-x.x.x-openEULER-64bit-cm.tar.gz` and configuration file `clusterconfig.xml` to the directory created in the previous step.

2. Decompress the installation package in the directory where the installation package `openGauss-x.x.x-openEULER-64bit.tar.gz` is stored. After the installation package is decompressed, the `script` directory is automatically generated in the `/opt/software/openGauss` directory. OM tool scripts such as `gs_preinstall` are generated in the `script` directory.

    ```
    cd /opt/software/openGauss
    tar -zxvf openGauss-x.x.x-openEULER-64bit.tar.gz
    ```

3. Go to the `script` directory.

    ```
    cd /opt/software/openGauss/script
    ```

4. If the openEuler OS is used, run the following command to open the `performance.sh` file, comment out `sysctl -w vm.min_free_kbytes=112640 &> /dev/null`, press `Esc` to switch to the command mode, and run the `:wq` command to save the modification and exit.

    ```
    vi /etc/profile.d/performance.sh
    ```

5. Before preinstallation, load the `lib` library in the installation package to ensure that the OpenSSL version is correct. In the following command, *{packagePath}* indicates the path to the installation package. In this example, the path is `/opt/software/openGauss`.

    ```
    export LD_LIBRARY_PATH={packagePath}/script/gspylib/clib:$LD_LIBRARY_PATH
    ```

6. Check whether the host name is the same as that in `/etc/hostname`. During preinstallation, the host name is checked.

7. Run `gs_preinstall` to configure the installation environment. If the environment is shared, add `--sep-env-file=*ENVFILE*` to separate environment variables to prevent mutual impact with other users. *ENVFILE* indicates the path to the environment variable separation file. Run the following command to execute `gs_preinstall` in interactive mode. During the execution, the trust between users `root` and between openGauss users is automatically created.

```
./gs_preinstall -U omm -G dbgrp -X /opt/software/openGauss/clusterconfig.xml
```

`omm` is the database administrator (that is, the OS user who runs openGauss), `dbgrp` is the group name of the OS user who runs openGauss, and `/opt/software/openGauss/clusterconfig.xml` is the path to the openGauss configuration file. You need to set up mutual trust as prompted and enter the password of the `root` or openGauss user.

### Performing Installation

After the openGauss installation environment is prepared by executing the pre-installation script, deploy openGauss and CM based on the installation process.

**Prerequisites**

- You have successfully executed the `gs_preinstall` command.
- All the server OSs and networks are functioning properly.
- The `locale` parameter for each server is set to the same value.

**Procedure**

1. (Optional) Check whether the installation package and openGauss configuration file exist in the planned path. If they do not exist, perform the preinstallation again. Ensure that the preinstallation is successful and then perform the following steps.

2. Log in to the openGauss host and switch to the `omm` user.

    ```
    su - omm
    ```

   > **Note**
   >
   > - `omm` is the user specified by the `-U` option in the `gs_preinstall` command.
   > - Run the `gs_install` command as user `omm`. Otherwise, an execution error is reported.

3. Run the `gs_install` command to install openGauss. If the cluster is installed in environment variable separation mode, run the `source` command to obtain the environment variable separation file `ENVFILE`.

    ```
    gs_install -X /opt/software/openGauss/clusterconfig.xml
    ```

    `/opt/software/openGauss/script/clusterconfig.xml` is the path to the openGauss configuration file. During the execution, you need to enter a database password as prompted. The password must meet complexity requirements. To ensure that you can use the database properly, remember the entered database password.

    The password must meet the following complexity requirements:

   - Contains at least eight characters.	
   - Cannot be the same as the user name, current password (ALTER), or the current password in reverse order.
   - Must contain at least three types of the following: uppercase letters (A to Z), lowercase letters (a to z), digits (0 to 9), and special characters ~!@#$%^&*()-_=+\|[{}];:,<.>/?

4. After the installation is successful, manually delete the trust between users `root` on the host, that is, delete the mutual trust file on each openGauss database node.

```
rm -rf ~/.ssh
```

### Uninstalling openGauss and CM

Uninstalling openGauss includes uninstalling openGauss and clearing the openGauss server environment.

#### **Uninstallation**

openGauss provides an uninstallation script to help you uninstall openGauss.

**Procedure**

1. Log in as the OS user `omm` to the primary database node.

2. Run the `gs_uninstall` command to uninstall openGauss.

```
gs_uninstall --delete-data
```

   Alternatively, uninstall openGauss on each node.

```
gs_uninstall --delete-data -L
```

#### **One-Click Environment Cleanup**

After openGauss is uninstalled, run the `gs_postuninstall` command to delete configurations from all openGauss servers if you do not need to re-deploy openGauss using these configurations. These configurations are made by the `gs_preinstall` script.

**Prerequisites**

- openGauss has been uninstalled.
- User `root` is trustworthy and available.
- Only user `root` is authorized to run the `gs_postuninstall` command.

**Procedure**

1. Log in to the openGauss server as user `root`.

2. Run the `ssh *Host name*` command to check whether mutual trust is established. Enter `exit` to exit.

   ```
   plat1:~ # ssh plat2 
   Last login: Tue Jan  5 10:28:18 2016 from plat1 
   plat2:~ # exit 
   logout 
   Connection to plat2 closed. 
   plat1:~ #
   ```

3. Go to the `script` directory.

   ```
   cd /opt/software/openGauss/script
   ```

4. Run `gs_postuninstall` to clear the environment configurations. If the cluster is installed in environment variable separation mode, run the `source` command to obtain the environment variable separation file `ENVFILE`.

   ```
   ./gs_postuninstall -U omm -X /opt/software/openGauss/clusterconfig.xml --delete-user --delete-group
   ```

  Alternatively, locally run `gs_postuninstall` on each openGauss node.

   ```
   ./gs_postuninstall -U omm -X /opt/software/openGauss/clusterconfig.xml --delete-user --delete-group -L
   ```

    `omm` is the name of the OS user who runs openGauss, and `/opt/software/openGauss/clusterconfig.xml` is the path to the openGauss configuration file.

    If the cluster is installed in environment variable separation mode, delete *ENVFILE* that is sourced and run `unset MPPDB_ENV_SEPARATE_PATH`.

5. Delete the trust between users `root` from each openGauss database node.

## License

[MulanPSL-2.0](http://license.coscl.org.cn/)
