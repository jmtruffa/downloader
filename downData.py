from CERDownloader import downloadCER
from A3500Downloader import downloadA3500
from ITCRMDownloader import downloadITCRM
from UVADownloader import downloadUVA

def main():

    print("Downloading CER data...")
    result_df2 = downloadCER()
    print("Downloading A3500 data...")
    result_a3500 = downloadA3500()
    print("Downloading ITCRM data...")
    result_itcrm = downloadITCRM()
    print("Downloading UVA data...")
    result_UVA = downloadUVA()

    # Process the result_df as needed
    #print(result_UVA)



if __name__ == "__main__": ## este código se ejecuta si se ejecuta como script
    main() 

