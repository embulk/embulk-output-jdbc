# gcc, g++ and Oracle Instant Client Basic and SDK are requred.
#
# ln libocci.so.x.x libocci.so
# ln libclntsh.so.x.x libclntsh.so
#

if [ "$OCI_PATH" = "" ]
then
    echo "You should set the environment variable 'OCI_PATH'."
    exit 1
fi

mkdir -p ../../../../lib/embulk/native/x86_64-linux

gcc -fPIC -I. -I"$OCI_PATH/sdk/include" -I../../../main/cpp/common -L"$OCI_PATH" -shared ../../../main/cpp/common/embulk-output-oracle-oci.cpp -locci -lclntsh -lstdc++ -o ../../../../lib/embulk/native/x86_64-linux/libembulk-output-oracle-oci.so
