from CERDownloader import downloadCER
from A3500Downloader import downloadA3500
from ITCRMDownloader import downloadITCRM
from UVADownloader import downloadUVA
from EMAEDownloader import downloadEMAE
from tableFuns import getTable
import serieseDownloader as bcra
import serieDiariaDownloader as serieDiaria

def main():

    # print("Downloading CER data...")
    # result_df2 = downloadCER()
    # print("Downloading A3500 data...")
    # result_a3500 = downloadA3500()
    # print("Downloading ITCRM data...")
    # result_itcrm = downloadITCRM()
    # print("Downloading UVA data...")
    # result_UVA = downloadUVA()
    #print("Downloading EMAE data...")
    #result_EMAE = downloadEMAE()

    # Process the result_df as needed
    #print(result_UVA)
    #print(result_EMAE)

    
    # table_df = getTable("A3500")

    # if table_df is not None:
    #      print(table_df)

    # print("Downloading SERIESE data...")
    # bcra.main()

    # bm_df = getTable("tasas")
    # print(bm_df)

    print("Downloading Serie Diaria data...")
    serieDiaria.serieDiaria()


if __name__ == "__main__": ## este código se ejecuta si se ejecuta como script
    main() 

