#!/bin/bash
# Stop on any errors
set -e
BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
DOWNLOAD_BASE="${1-$BASE_DIR}"
echo "Using ${DOWNLOAD_BASE} for downloads"
DPDK_VER=${DPDK_VER-"17.08"}
MODE=download  # or git
DOWNLOAD_PATH="${DOWNLOAD_BASE}/dpdk-${DPDK_VER}.tar.xz"
DPDK_RESULT="${BASE_DIR}/dpdk"
CONFIG_FILE=${DPDK_CONFIG_FILE-"${BASE_DIR}/dpdk-confs/common_linuxapp-${DPDK_VER}"}
CONFIG_PFX=${DPDK_CONFIG_PFX-""}
echo "Using configuration ${CONFIG_FILE}${CONFIG_PFX}"

if [ "$MODE" = "download" ]; then
    if [ ! -e "$DOWNLOAD_PATH" ]; then
        echo "Fetching https://fast.dpdk.org/rel/dpdk-${DPDK_VER}.tar.xz"
        curl -L "https://fast.dpdk.org/rel/dpdk-${DPDK_VER}.tar.xz" -o "${DOWNLOAD_PATH}"
    fi
    if [ ! -d "${DPDK_RESULT}" ]; then
        mkdir -p "${DPDK_RESULT}"
    fi
    tar -xJvf "${DOWNLOAD_PATH}" -C "${DPDK_RESULT}" --strip-components=1
else
    DPDK_REV="2e14846d15addd349a909176473e936f0cf79075"
    if [ ! -d "${DPDK_RESULT}" ]; then
        git clone git://dpdk.org/dpdk "${DPDK_RESULT}"
        pushd "${DPDK_RESULT}"
        git checkout "${DPDK_REV}"
        popd
    fi
fi

cp "${CONFIG_FILE}${CONFIG_PFX}" "${DPDK_RESULT}/config/common_linuxapp"
export RTE_TARGET=x86_64-native-linuxapp-gcc
FLAGS="-g3 -Wno-error=maybe-uninitialized -fPIC"

make config -C "${DPDK_RESULT}" T=x86_64-native-linuxapp-gcc EXTRA_CFLAGS="$FLAGS"
PROCS="$(nproc)"
make -j "${PROCS}" -C "${DPDK_RESULT}" EXTRA_CFLAGS="$FLAGS"
