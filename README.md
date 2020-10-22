katsdpdata
==========

This package serves as a placeholder for the data tranfer script, which performs metadata extraction and transfers MeerKAT data products from the LOP (Link Out Protection) buffer to the MTPA (Medium Term Preservation Archive) at the CHCP.

It also contains legacy metadata extractor code for use with KAT7, RTS and MeerKATAR1 .h5 files.

LOP to MTPA transfer script:
* scripts/vis_trawler.py 

Stand alone metadata extractor:
* scripts/tel_prod_met_extractor.py {.h5/.rdb} - creates a CAS format (xml) .met file containing extracted metadata.

Download utilities for use with MTPA:
* scripts/download_cbid_prods.py
* scripts/download_cbid_prods_maximum_plaid.py
