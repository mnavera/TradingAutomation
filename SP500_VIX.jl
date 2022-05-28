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



    println("-----------------S&P500 VIX-------------------")

    VIX=LittleDict(
        "SP500_VIX_daily"=>"https://api.stlouisfed.org/fred/series/observations?series_id=VIXCLS&file_type=json&aggregation_method=eop&output_type=1&sort_order=desc",
        "SP500_VIX_weekly"=>"https://api.stlouisfed.org/fred/series/observations?series_id=VIXCLS&file_type=json&aggregation_method=eop&output_type=1&sort_order=desc&frequency=wesu",
        "SP500_VIX_monthly"=>"https://api.stlouisfed.org/fred/series/observations?series_id=VIXCLS&file_type=json&aggregation_method=eop&output_type=1&sort_order=desc&frequency=m"
    )
    fpath=FILEPATH*"/SP500_VIX/"

    @sync begin
        println("[+] Getting Data for VIX...")
        for (key, url) in VIX
            @async begin
                println("   [+] Getting "*key)
                final_vix_url=url*"&api_key="*FRED_API_KEY
                try
                    data=parseFREDData(final_vix_url)
                    fname = fpath*key

                    #Change value for dates with no data to missing
                    #data[data.Value.==".",:Value] .= missing

                    #write to csv
                    println("\n[+] Writing to "*key*".csv...")
                    CSV.write(fname*".csv",data)
                catch e
                    println(e)
                end
            end
        end
    end
end

main()