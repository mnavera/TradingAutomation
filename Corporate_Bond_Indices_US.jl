using HTTP
using DataFrames
using CSV
using Dates
using JSON
using OrderedCollections

include("ENV.jl")
include("WebParsers.jl")

using .WebParsers

function main()
    println("-----------------Corporate Bond Indices US-------------------")
    #Get today's date and one year ago in epoch time
    CURRENT_EPOCH=Int(floor(Dates.datetime2unix(DateTime(Dates.now()))))
    ONEYR_EPOCH=CURRENT_EPOCH-31536000
    OBSERVATION_START=Date(Dates.unix2datetime(ONEYR_EPOCH))


    #10 Year Treasury Daily Value
    DGS10="https://api.stlouisfed.org/fred/series/observations?series_id=DGS10&file_type=json&aggregation_method=eop&output_type=1&sort_order=desc"

    #CORPBONDS URL format(FRED): https://api.stlouisfed.org/fred/series/observations?series_id=BAMLC0A1CAAAEY&file_type=json&aggregation_method=eop&output_type=1&api_key=FRED_API_KEY
    CORPBONDS=LittleDict{String,String}(
        "AAA"=>"https://api.stlouisfed.org/fred/series/observations?series_id=BAMLC0A1CAAAEY&file_type=json&aggregation_method=eop&output_type=1&sort_order=desc",
        "BBB"=>"https://api.stlouisfed.org/fred/series/observations?series_id=BAMLC0A4CBBBEY&file_type=json&aggregation_method=eop&output_type=1&sort_order=desc",
        "CCC"=>"https://api.stlouisfed.org/fred/series/observations?series_id=BAMLH0A3HYCEY&file_type=json&aggregation_method=eop&output_type=1&sort_order=desc",
    )

    #ETF URL format(YFINANCE): https://query1.finance.yahoo.com/v7/finance/download/TLT?interval=1d&events=history&includeAdjustedClose=true&period1=CURRENT_EPOCH&period2=ONEYR_EPOCH
    ETFURLS=LittleDict{String,String}(
        "TLT"=>"https://query1.finance.yahoo.com/v7/finance/download/TLT?interval=1d&events=history&includeAdjustedClose=true",
        "SHY"=>"https://query1.finance.yahoo.com/v7/finance/download/SHY?interval=1d&events=history&includeAdjustedClose=true",
        "LQD"=>"https://query1.finance.yahoo.com/v7/finance/download/LQD?interval=1d&events=history&includeAdjustedClose=true",
        "JNK"=>"https://query1.finance.yahoo.com/v7/finance/download/JNK?interval=1d&events=history&includeAdjustedClose=true",
        "HYG"=>"https://query1.finance.yahoo.com/v7/finance/download/HYG?interval=1d&events=history&includeAdjustedClose=true"

    )


    #Delete old CSVs in directory before adding in new ones
    #rm(FILEPATH, recursive=true, force=true)
    #mkpath(FILEPATH)

    final_fred_df=DataFrame()

    println("\n","---Getting FRED Data---", "\n")

    #Get Daily 10 Year Treasury Bond Data from FRED API
    println("[+] Getting Data for DGS10...")
    res=parseFREDData(DGS10*"&observation_start="*string(OBSERVATION_START)*"&api_key="*FRED_API_KEY)
    


    #GET Corp Bond Data from FRED API
    for (k,url) in CORPBONDS
        println("[+] Getting Data for ",k,"...")
        res=parseFREDData(url*"&observation_start="*string(OBSERVATION_START)*"&api_key="*FRED_API_KEY)
        #println(res)
        date_col="Date_"*k
        value_col="Value_"*k
        final_fred_df[!, date_col] = res.Date
        final_fred_df[!, value_col] = res.Value
        #f = "FRED_"*k
    end


    
    #Get CSV Data from YFINANCE
    println("\n","---Getting YFINANCE Data---", "\n")
    for (k,url) in ETFURLS
        println("[+] Getting Data for ETF ",k,"...")
        res=parseYFData(url*"&period1="*string(ONEYR_EPOCH)*"&period2="*string(CURRENT_EPOCH))
        f = FILEPATH*"/Corporate_Bond_Indices_US/"*"ETF_"*k*".csv"
        CSV.write(f,res)
    end

    #write FRED data to file
    CSV.write(FILEPATH*"/Corporate_Bond_Indices_US/"*"Corporate_Bond_Indices_US.csv",final_fred_df)
end

main()
