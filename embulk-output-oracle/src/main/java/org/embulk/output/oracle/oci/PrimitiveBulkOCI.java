package org.embulk.output.oracle.oci;

import jnr.ffi.Pointer;
import jnr.ffi.types.u_int16_t;
import jnr.ffi.types.u_int32_t;

public class PrimitiveBulkOCI implements BulkOCI {

    private final OCI oci;


    public PrimitiveBulkOCI(OCI oci)
    {
        this.oci = oci;
    }

    public short embulk_output_oracle_OCIDirPathColArrayEntriesSet(
            Pointer dpca,
            Pointer errhp,
            @u_int16_t short columnCount,
            @u_int32_t int rowCount,
            Pointer data,
            Pointer sizes) {

        int index = 0;
        long offset = 0;
        for (int row = 0; row < rowCount; row++) {
            for (short column = 0; column < columnCount; column++) {
                short size = sizes.getShort(index++ * 2);
                short result = oci.OCIDirPathColArrayEntrySet(
                        dpca,
                        errhp,
                        row,
                        column,
                        data.slice(offset),
                        size,
                        OCI.OCI_DIRPATH_COL_COMPLETE);
                if (result != OCI.OCI_SUCCESS) {
                    return result;
                }
                offset += size;
            }
        }

        return OCI.OCI_SUCCESS;
    }
}
