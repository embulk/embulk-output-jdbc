@ECHO OFF

REM  Visual Studio and Oracle Instant Client SDK are requred.
REM  You should set the environment variable 'PATH' to CL.exe(x86_amd64) of Visual Studio.

IF "%JAVA_HOME%" == "" (
    ECHO "You should set the environment variable 'JAVA_HOME'."
    EXIT /B 1
)


IF "%OCI_SDK_PATH%" == "" (
    ECHO "You should set the environment variable 'OCI_SDK_PATH'."
    EXIT /B 1
)

IF "%MSVC_PATH%" == "" (
    ECHO "You should set the environment variable 'MSVC_PATH'."
    ECHO "For example : SET MSVC_PATH=C:\Program Files (x86)\Microsoft Visual Studio 10.0\VC"
    EXIT /B 1
)

IF "%MSSDK_PATH%" == "" (
    ECHO "You should set the environment variable 'MSSDK_PATH'."
    ECHO "For example : SET MSSDK_PATH=C:\Program Files (x86)\Microsoft SDKs\Windows\v7.0A"
    EXIT /B 1
)


MKDIR ..\..\..\..\lib\embulk\win_x64

CL /I"%MSSDK_PATH%\Include" /I"%MSVC_PATH%\include" /I"%OCI_SDK_PATH%\include" /I"%JAVA_HOME%\include" /I"%JAVA_HOME%\include\win32"  /Zi /nologo /W3 /WX- /O2 /Oi /GL /D "WIN32" /D "NDEBUG" /D "_WINDOWS" /D "_USRDLL" /D "EMBULKOUTPUTORACLE_EXPORTS" /D "_WINDLL" /D "_UNICODE" /D "UNICODE" /Gm- /EHsc /GS /Gy /fp:precise /Zc:wchar_t /Zc:forScope /Gd /errorReport:queue ..\common\embulk-output-oracle.cpp ..\common\dir-path-load.cpp dllmain.cpp /link /LIBPATH:"%MSVC_PATH%\lib\amd64" /LIBPATH:"%MSSDK_PATH%\Lib\x64" /LIBPATH:"%OCI_SDK_PATH%\lib\msvc" /INCREMENTAL:NO /NOLOGO /LIBPATH:"%OCI_SDK_PATH%\lib\msvc" /OUT:"..\..\..\..\lib\embulk\win_x64\embulk-output-oracle.dll" /DLL "oci.lib" "kernel32.lib" "user32.lib" "gdi32.lib" "winspool.lib" "comdlg32.lib" "advapi32.lib" "shell32.lib" "ole32.lib" "oleaut32.lib" "uuid.lib" "odbc32.lib" "odbccp32.lib" /SUBSYSTEM:WINDOWS /OPT:REF /OPT:ICF /LTCG /TLBID:1 /DYNAMICBASE /NXCOMPAT /MACHINE:X64 /ERRORREPORT:QUEUE