## Esta servicio deberá descargar el CER desde la página del BCRA y guardarlo en la base de datos.


from CERDownloaderOld import downloadCER
from CERDownloader import downloadCER
from A3500Downloader import downloadA3500
from ITCRMDownloader import downloadITCRM

def main():

    # URL of the XLS file
    
    # Call the function to download and read the XLS file
    #result_df = downloadCER()

    result_df2 = downloadCER()
    result_a3500 = downloadA3500()
    result_itcrm = downloadITCRM()

    # Process the result_df as needed
    



if __name__ == "__main__": ## este código se ejecuta si se ejecuta como script
    main() 

