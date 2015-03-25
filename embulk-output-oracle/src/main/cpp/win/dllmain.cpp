#pragma once

// SDKDDKVer.h をインクルードすると、利用できる最も上位の Windows プラットフォームが定義されます。

// 以前の Windows プラットフォーム用にアプリケーションをビルドする場合は、WinSDKVer.h をインクルードし、
// SDKDDKVer.h をインクルードする前に、サポート対象とするプラットフォームを示すように _WIN32_WINNT マクロを設定します。

#include <SDKDDKVer.h>

#define WIN32_LEAN_AND_MEAN             // Windows ヘッダーから使用されていない部分を除外します。
// Windows ヘッダー ファイル:
#include <windows.h>


BOOL APIENTRY DllMain( HMODULE hModule,
                       DWORD  ul_reason_for_call,
                       LPVOID lpReserved
					 )
{
	switch (ul_reason_for_call)
	{
	case DLL_PROCESS_ATTACH:
	case DLL_THREAD_ATTACH:
	case DLL_THREAD_DETACH:
	case DLL_PROCESS_DETACH:
		break;
	}
	return TRUE;
}

