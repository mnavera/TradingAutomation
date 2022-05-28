using DataFrames
using ExcelFiles
using Dates
using OrderedCollections
using HTTP
using ZipFile

include("../ENV.jl")


function getCotReport(year::String,dir::String)
    #download CoT report CSV from internet
    url="https://www.cftc.gov/sites/default/files/files/dea/history/fut_disagg_xls_"*year*".zip"
    r=HTTP.request("GET", url; verbose=0)

    #unzip file
    zipfile=ZipFile.Reader(IOBuffer(r.body))
    f=zipfile.files[1]
    
    fname=dir*"f_year.xls"

    #write xls file
    write(fname,f)


    #load XLS into DataFrame
    df=DataFrame(load(fname,"XLS"))

    #remove xls file; we don't need it anymore 
    rm(fname,force=true)
    
    return df
end

function main()
    println("-----------------Commitment of Traders-------------------")
    #Location for upload
    fpath=FILEPATH*"/Cyclical_Commodities/"
    curr_dir=pwd()*"/Commitment_of_Traders/"

    SOURCECOLS=LittleDict{String,String}(
        "WTI_OIL"=>"CRUDE OIL, LIGHT SWEET-WTI - ICE FUTURES EUROPE",
        "BRENT_OIL"=>"BRENT CRUDE OIL LAST DAY - NEW YORK MERCANTILE EXCHANGE",
        "HEAT_OIL"=>"#2 HEATING OIL- NY HARBOR-ULSD - NEW YORK MERCANTILE EXCHANGE",
        "PALLADIUM"=>"PALLADIUM - NEW YORK MERCANTILE EXCHANGE",
        "PLATINUM"=>"PLATINUM - NEW YORK MERCANTILE EXCHANGE",
        "SILVER"=>"SILVER - COMMODITY EXCHANGE INC.",
        "GOLD"=>"GOLD - COMMODITY EXCHANGE INC.",
        "COPPER"=>"COPPER-GRADE #1 - COMMODITY EXCHANGE INC.",
        "ALUMINUM"=>"ALUMINUM MW US TR PLATTS - COMMODITY EXCHANGE INC.",
        "STEEL"=>"US MIDWEST DOMESTIC HOT-ROLL  - COMMODITY EXCHANGE INC.",
        "NAT_GAS"=>"NATURAL GAS - NEW YORK MERCANTILE EXCHANGE"
    )
    
    COLNAMES=[:Market_and_Exchange_Names,:Report_Date_as_MM_DD_YYYY,:Open_Interest_All,:M_Money_Positions_Long_ALL,:M_Money_Positions_Short_ALL]
    
    #get CoT Report Data Blob
    #we only wanna get the columns we're interested in
    working_df=getCotReport("2022",curr_dir)[:,COLNAMES]

    
    #working_df=input_df[:,COLNAMES]
    
    #dataframe we'll be putting our dataa in in the end
    
    final_df=DataFrame(Market_and_Exchange_Names=Any[],Report_Date_as_MM_DD_YYYY=Any[],Open_Interest_All=Any[],M_Money_Positions_Long_ALL=Any[],M_Money_Positions_Short_ALL=Any[])
    
 
    for (k,val) in SOURCECOLS
        df=filter(COLNAMES[1]=>n->n == val, working_df)
        println("[+] Getting Data for "*k*"...")
        #Sort from oldest to newest
        sort!(df,[:Report_Date_as_MM_DD_YYYY])
        append!(final_df,df)
    

    end

    println("Writing to file...")
    filename=fpath*"Commitment_of_Traders.xlsx"

    #delete existing file if it already exists
    rm(filename,force=true)
    save(filename,final_df)


end

main()