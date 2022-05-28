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
    println("-----------------US Benchmark Yields-------------------")
    #Location for upload
    fpath=FILEPATH*"/Benchmark_Yields_US/"

    #Get today's date and one year ago in epoch time
    CURRENT_EPOCH=Int(floor(Dates.datetime2unix(DateTime(Dates.now()))))
    ONEYR_EPOCH=CURRENT_EPOCH-31536000
    OBSERVATION_START=Date(Dates.unix2datetime(ONEYR_EPOCH))

    YIELDURLS=LittleDict{String,String}(
    "FFUNDS"=>"https://api.stlouisfed.org/fred/series/observations?series_id=DFF&file_type=json&frequency=wesu&aggregation_method=eop&output_type=1",
    "DGS1MO"=>"https://api.stlouisfed.org/fred/series/observations?series_id=DGS1MO&file_type=json&frequency=wesu&aggregation_method=eop&output_type=1",
    "DGS3MO"=>"https://api.stlouisfed.org/fred/series/observations?series_id=DGS3MO&file_type=json&frequency=wesu&aggregation_method=eop&output_type=1",
    "DGS6MO"=>"https://api.stlouisfed.org/fred/series/observations?series_id=DGS6MO&file_type=json&frequency=wesu&aggregation_method=eop&output_type=1",
    "DGS1"=>"https://api.stlouisfed.org/fred/series/observations?series_id=DGS1&file_type=json&frequency=wesu&aggregation_method=eop&output_type=1",
    "DGS2"=>"https://api.stlouisfed.org/fred/series/observations?series_id=DGS2&file_type=json&frequency=wesu&aggregation_method=eop&output_type=1",
    "DGS3"=>"https://api.stlouisfed.org/fred/series/observations?series_id=DGS3&file_type=json&frequency=wesu&aggregation_method=eop&output_type=1",
    "DGS5"=>"https://api.stlouisfed.org/fred/series/observations?series_id=DGS5&file_type=json&frequency=wesu&aggregation_method=eop&output_type=1",
    "DGS7"=>"https://api.stlouisfed.org/fred/series/observations?series_id=DGS7&file_type=json&frequency=wesu&aggregation_method=eop&output_type=1",
    "DGS10"=>"https://api.stlouisfed.org/fred/series/observations?series_id=DGS10&file_type=json&frequency=wesu&aggregation_method=eop&output_type=1",
    "DGS20"=>"https://api.stlouisfed.org/fred/series/observations?series_id=DGS20&file_type=json&frequency=wesu&aggregation_method=eop&output_type=1",
    "DGS30"=>"https://api.stlouisfed.org/fred/series/observations?series_id=DGS10&file_type=json&frequency=wesu&aggregation_method=eop&output_type=1",
    "DFII5"=>"https://api.stlouisfed.org/fred/series/observations?series_id=DFII5&file_type=json&frequency=wesu&aggregation_method=eop&output_type=1",
    "DFII7"=>"https://api.stlouisfed.org/fred/series/observations?series_id=DFII7&file_type=json&frequency=wesu&aggregation_method=eop&output_type=1",
    "DFII10"=>"https://api.stlouisfed.org/fred/series/observations?series_id=DFII10&file_type=json&frequency=wesu&aggregation_method=eop&output_type=1",
    "DFII20"=>"https://api.stlouisfed.org/fred/series/observations?series_id=DFII20&file_type=json&frequency=wesu&aggregation_method=eop&output_type=1",
    "DFII30"=>"https://api.stlouisfed.org/fred/series/observations?series_id=DFII30&file_type=json&frequency=wesu&aggregation_method=eop&output_type=1",
    )


    #r=HTTP.request("GET", FRED_NON_API_URL_BOND_TIPS; verbose=2)
    #println(r.status)
    #println(String(r.body))



    #Delete old CSVs in directory before adding in new ones
    #rm(FILEPATH, recursive=true, force=true)
    #mkpath(FILEPATH)

    final_df=DataFrame()

    println("\n","---Getting FRED Data---", "\n")
    for (k,url) in YIELDURLS
        println("[+] Getting Data for ",k,"...")
        final_url=url*"&observation_start="*string(OBSERVATION_START)*"&api_key="*FRED_API_KEY
        data=parseFREDData(final_url)

        #populate Date column first
        if isempty(final_df)
            final_df[!, :Date] = data.Date
        end

        final_df[!, k] = data.Value

        #println(final_df)
    end

        #Write to file
        filename=fpath*"Benchmark_Yields_US"*"_"*string(Dates.today())*".csv"
        CSV.write(filename,final_df)
end

main()
