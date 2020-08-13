#!/bin/dash
#  The following script is an example template which can be used to automate the collection of configuration data specific to the Genesys installation. This script is work in progress.   run it

#set -x
#Function to execute a command and store stdout and stderr to file
collect_data () {
  path=collect/${1}
  cmd=${@:2}
  file=`hostname`-${cmd//[^A-Za-z0-9._-]/_}
  mkdir -p $path
  eval $cmd 2> $path/$file.stderr 1> $path/$file.stdout
  gzip $path/$file.stderr $path/$file.stdout
}


#Function to collect, tar, compress, and store files
collect_files() {
  path=collect/${1}
  file=`hostname`-${2}
  find_args=${@:3}
  mkdir -p $path
  find $find_args | tar cf $path/$file.tar -T - 2> $path/$file.stderr 1> $path/$file.stdout
  gzip $path/$file.tar $path/$file.stderr $path/$file.stdout
}


# Information about CPUs
collect_data  CPU          lscpu
collect_data  CPU          cat /proc/cpuinfo
collect_data  CPU          nproc

# Information about memory
#collect-data  MEMORY       cat /proc/meminfo

# Information about storage
collect_data  STORAGE      blkid
collect_data  STORAGE      lsblk -b
collect_data  STORAGE      lsblk -f
collect_data  STORAGE      mount
collect_data  STORAGE      findmnt
collect_data  STORAGE      cat /etc/fstab
collect_data  STORAGE      cat /etc/mtab
collect_data  STORAGE      cat /proc/mounts
collect_data  STORAGE      df -Pa
collect_data  STORAGE      df -i

# Information about network configuration
collect_data  NETWORK      ip addr show
collect_data  NETWORK      ip route show
collect_data  NETWORK      cat /etc/hostname
collect_data  NETWORK      cat /etc/hosts
collect_data  NETWORK      cat /etc/resolv.conf
collect_data  NETWORK      cat /etc/nsswitch.conf

# Information about kernel
#collect_data  OS/KERNEL    uname -a
#collect_data  OS/KERNEL    cat /proc/version

# Information about OS release
#collect_data  OS/RELEASE   cat /etc/os-release
#collect_data  OS/RELEASE   hostnamectl

# Information about OS packages
#collect_data  OS/PACKAGES  yum repolist -v
#collect_data  OS/PACKAGES  yum list installed
#collect_data  OS/PACKAGES  yum history
#collect_data  OS/PACKAGES  rpm -qa --last

# Information about services
#collect_data  SERVICES     systemctl status --no-page
#collect_data  SERVICES     systemctl list-units --all --no-page
#collect_data  SERVICES     systemctl list-sockets --no-page
#collect_data  SERVICES     systemctl list-timers --no-page
#collect_data  SERVICES     systemctl list-dependencies --no-page
#collect_data  SERVICES     systemctl list-unit-files --all --no-page
#collect_data  SERVICES     systemctl list-machines --no-page
#collect_data  SERVICES     systemctl list-jobs --no-page
#collect_data  SERVICES     systemctl --full --type service --all  --no-page
#collect_data  SERVICES     ls -ltr /etc/init.d/
#collect_data  SERVICES     service --status-all

# Information about Users & Groups
#collect_data  USERS        cat /etc/passwd
#collect_data  USERS        cat /etc/group

# Information about running processes
#collect_data  PROCESSES    ps auxf

# Information about listening TCP & UDP ports
#collect_data  NETWORK      netstat -lntup
#collect_data  NETWORK      ss -lntu

# Information about system kernel limits
#collect-files  ETC         security-limits  /etc/security/limits.*

# Information about Genesys systemd scripts
#collect-files  ETC         genesys-systemd-scripts  /etc/systemd/system/gcti*

# Information about Genesys files installed in /opt/genesys
#collect_data  GENESYS      ls -lRa /opt/genesys

# Information about any Genesys config files in /opt/genesys
#collect-files  GENESYS     config-files  /opt/genesys -name "*.cfg" -o -name "*.ini" -name "*.sh" -o -name "*.xml"

# Information about Genesys user profiles
#collect-files  GENESYS     user-profiles  /home/*genesys* -name ".profile" -o -name ".bashrc"

# Information about Oracle files installed in /opt/*ora*
#collect_data  ORACLE       ls -lR /opt/*ora*

# Information about Oracle sqlnet.ora or tnsnames.ora files in /opt/*ora*
#collect-files  ORACLE     client-config  /opt/*ora* -name "sqlnet.ora" -o -name "tnsnames.ora"

