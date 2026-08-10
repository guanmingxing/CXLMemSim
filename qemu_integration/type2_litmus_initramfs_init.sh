#!/bin/sh
set -eu

PATH=/bin:/usr/bin:/sbin:/usr/sbin
export PATH

litmus_status=1

shutdown_guest() {
    echo "TYPE2_INIT_EXIT=$litmus_status"
    sync || true
    poweroff -f || true
    while :; do
        sleep 1
    done
}

trap shutdown_guest EXIT

find_dax_device() {
    dax_path=
    for candidate in /dev/dax*; do
        if [ -c "$candidate" ]; then
            dax_path=$candidate
            return 0
        fi
    done
    return 1
}

mount -t proc proc /proc
mount -t sysfs sysfs /sys
mount -t devtmpfs devtmpfs /dev
mkdir -p /dev/pts /run /tmp
mount -t devpts devpts /dev/pts || true

iterations=128
for option in $(cat /proc/cmdline); do
    case "$option" in
        type2_iterations=*) iterations=${option#type2_iterations=} ;;
    esac
done

echo "TYPE2_INIT_START iterations=$iterations"

memdev_path=
attempt=0
while [ "$attempt" -lt 60 ]; do
    for candidate in /sys/bus/cxl/devices/mem*; do
        if [ -e "$candidate" ]; then
            memdev_path=$candidate
            break
        fi
    done
    [ -n "$memdev_path" ] && break
    attempt=$((attempt + 1))
    sleep 1
done
if [ -z "$memdev_path" ]; then
    echo "TYPE2_INIT_ERROR=no-cxl-memdev"
    dmesg
    exit 1
fi
memdev=${memdev_path##*/}

decoder_path=
for candidate in /sys/bus/cxl/devices/decoder0.*; do
    if [ -e "$candidate" ]; then
        decoder_path=$candidate
        break
    fi
done
if [ -z "$decoder_path" ]; then
    echo "TYPE2_INIT_ERROR=no-root-decoder"
    /usr/bin/cxl list -B -D -M || true
    exit 1
fi
decoder=${decoder_path##*/}

echo "TYPE2_INIT_CXL memdev=$memdev decoder=$decoder"
/usr/bin/cxl list -B -D -M || true
if ! /usr/bin/cxl create-region -m -t ram -d "$decoder" -w 1 -g 1024 -s 256M "$memdev"; then
    echo "TYPE2_INIT_ERROR=create-region"
    /usr/bin/cxl list -B -D -M -R || true
    dmesg
    exit 1
fi

region_path=
attempt=0
while [ "$attempt" -lt 30 ]; do
    for candidate in /sys/bus/cxl/devices/region*; do
        if [ -e "$candidate" ]; then
            region_path=$candidate
            break
        fi
    done
    [ -n "$region_path" ] && break
    attempt=$((attempt + 1))
    sleep 1
done
if [ -z "$region_path" ]; then
    echo "TYPE2_INIT_ERROR=no-region"
    exit 1
fi
region=${region_path##*/}

dax_path=
if ! find_dax_device; then
    if ! /usr/bin/daxctl create-device -r "$region"; then
        echo "TYPE2_INIT_ERROR=create-dax"
        /usr/bin/daxctl list || true
        exit 1
    fi
fi

attempt=0
while [ "$attempt" -lt 30 ]; do
    find_dax_device && break
    attempt=$((attempt + 1))
    sleep 1
done
if [ -z "$dax_path" ]; then
    echo "TYPE2_INIT_ERROR=no-dax-device"
    /usr/bin/daxctl list || true
    exit 1
fi

bar_path=
pci_bdf=
for device_dir in /sys/bus/pci/devices/*; do
    if [ -r "$device_dir/vendor" ] && [ -r "$device_dir/device" ] &&
       [ "$(cat "$device_dir/vendor")" = "0x8086" ] &&
       [ "$(cat "$device_dir/device")" = "0x0d92" ] &&
       [ -e "$device_dir/resource2" ]; then
        bar_path=$device_dir/resource2
        pci_bdf=${device_dir##*/}
        break
    fi
done
if [ -z "$bar_path" ]; then
    echo "TYPE2_INIT_ERROR=no-type2-bar2"
    dmesg
    exit 1
fi

echo "TYPE2_INIT_TOPOLOGY pci=$pci_bdf region=$region dax=$dax_path bar2=$bar_path"
/usr/bin/cxl list -B -D -M -R || true
/usr/bin/daxctl list || true

set +e
/bin/type2_device_litmus.static \
    --dax "$dax_path" \
    --bar "$bar_path" \
    --iterations "$iterations" \
    --map-size 2097152 \
    --base-offset 65536 \
    >/tmp/type2-guest.json 2>/tmp/type2-runner.log
litmus_status=$?
set -e

cat /tmp/type2-runner.log
echo "TYPE2_LITMUS_JSON_BEGIN"
cat /tmp/type2-guest.json
echo "TYPE2_LITMUS_JSON_END"
exit "$litmus_status"
