build() {
  echo "RUN: ../gradlew gem"
  ../gradlew gem
  echo "RUN: embulk gem install --local build/gems/embulk-output-redshift-0.9.0-java.gem"
  embulk gem install --local build/gems/embulk-output-redshift-0.9.0-java.gem
  # echo "succeed in building and installing"
}

run() {
  echo "RUN: embulk -J-Xmx2048m -J-Dio.netty.leakDetection.level=advanced run $1"
  embulk \
    -J-Xmx2048m \
    -J-Dio.netty.leakDetection.level=advanced \
    -J-Djava.io.tmpdir=/Users/xuweida/Desktop/primeNumber/embulk-output-jdbc/embulk-output-redshift/tmp \
    run $1
}

case $1 in
  "" )
    echo "Please pass an argument"
    ;;
  "build" )
    echo "build mode"
    build
    ;;
  "run" )
    echo "run mode with default file"
    [[ $2 = "-build" ]] && build
    run "small.yaml"
    ;;
  * )
    echo "run mode with specified file"
    [[ $2 = "-build" ]] && build
    run $1
    ;;
esac
exit 0
