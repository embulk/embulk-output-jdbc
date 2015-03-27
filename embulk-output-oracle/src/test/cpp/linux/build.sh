# gcc, g++ and Oracle Instant Client SDK are requred.
#
# ln libocci.so.x.x libocci.so
# ln libclntsh.so.x.x libclntsh.so
#

if [ "$JAVA_HOME" = "" ]
then
    echo "You should set the environment variable 'JAVA_HOME'."
    exit 1
fi

if [ "$OCI_PATH" = "" ]
then
    echo "You should set the environment variable 'OCI_PATH'."
    exit 1
fi

gcc -I. -I"$JAVA_HOME/include" -I"$OCI_PATH/sdk/include" -I../../../main/cpp/common -L"$OCI_PATH" ../common/embulk-output-oracle-test.cpp ../../../main/cpp/common/embulk-output-oracle.cpp ../../../main/cpp/common/dir-path-load.cpp -locci -lclntsh -lstdc++ -o embulk-output-oracle-test
