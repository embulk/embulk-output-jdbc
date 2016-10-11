#include <occi.h>


extern "C" 
#ifdef WIN32
    __declspec(dllexport) 
#endif 
sword embulk_output_oracle_OCIDirPathColArrayEntriesSet(
        OCIDirPathColArray *dpca,
        OCIError *errhp,
        ub2 columnCount,
        ub4 rowCount,
        ub1* data,
        ub2* sizes)
{
    for (ub4 row = 0; row < rowCount; row++) {
        for (ub2 column = 0; column < columnCount; column++) {
            ub2 size = *sizes++;
            sword result = OCIDirPathColArrayEntrySet(dpca, errhp, row, column, data, size, OCI_DIRPATH_COL_COMPLETE);
            if (result != OCI_SUCCESS) {
                return result;
            }
            data += size;
        }
    }
    return OCI_SUCCESS;
}
