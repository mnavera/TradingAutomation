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
    println("-----------------Census Bureau Housing Survey-------------------")
    #Get today's date and one year ago in epoch time
    CURRENT_EPOCH=Int(floor(Dates.datetime2unix(DateTime(Dates.now()))))
    TWOYR_EPOCH=CURRENT_EPOCH-(31536000 * 2)
    OBSERVATION_START=Date(Dates.unix2datetime(TWOYR_EPOCH))
    
    #Location for upload
    fpath=FILEPATH*"/Census_Bureau_Housing_Survey/"
    

    HOUSING=LittleDict{String,String}(
        "PERMIT"=>"https://api.stlouisfed.org/fred/series/observations?series_id=PERMIT&file_type=json&aggregation_method=eop&output_type=1&sort_order=desc",
        "HOUST"=>"https://api.stlouisfed.org/fred/series/observations?series_id=HOUST&file_type=json&aggregation_method=eop&output_type=1&sort_order=desc",
        "COMPUTSA"=>"https://api.stlouisfed.org/fred/series/observations?series_id=COMPUTSA&file_type=json&aggregation_method=eop&output_type=1&sort_order=desc"
    )

   
    final_fred_df=DataFrame()

    for (k,url) in HOUSING
        println("[+] Getting Data for ",k,"...")
        #res=parseFREDData(url*"&observation_start="*string(OBSERVATION_START)*"&api_key="*FRED_API_KEY)
        res=parseFREDData(url*"&observation_start="*string(OBSERVATION_START)*"&api_key="*FRED_API_KEY)
        
        date_col="Date_"*k
        value_col="Value_"*k
        final_fred_df[!, date_col] = res.Date
        final_fred_df[!, value_col] = res.Value
        #f = "FRED_"*k
    end
    CSV.write(fpath*"Housing_Surveys"*"_"*string(Dates.today())*".csv",final_fred_df)
end



main()