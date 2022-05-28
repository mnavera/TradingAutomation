using HTTP
using DataFrames
using CSV
using Dates
using JSON
using OrderedCollections
using XLSX

include("ENV.jl")
include("WebParsers.jl")

using .WebParsers


function main()

    println("-----------------Consumer and Producer Prices-------------------")
    #Get today's date and one year ago in epoch time
    CURRENT_EPOCH=Int(floor(Dates.datetime2unix(DateTime(Dates.now()))))
    ONEYR_EPOCH=CURRENT_EPOCH-31536000
    OBSERVATION_START=Date(Dates.unix2datetime(ONEYR_EPOCH))


    PRICES=LittleDict{String,String}(
        "CPIAUCSL"=>"https://api.stlouisfed.org/fred/series/observations?series_id=CPIAUCSL&file_type=json&aggregation_method=eop&output_type=1&sort_order=desc",
        "CPILFESL"=>"https://api.stlouisfed.org/fred/series/observations?series_id=CPILFESL&file_type=json&aggregation_method=eop&output_type=1&sort_order=desc",
        "PCEPI"=>"https://api.stlouisfed.org/fred/series/observations?series_id=PCEPI&file_type=json&aggregation_method=eop&output_type=1&sort_order=desc",
        "PCEPILFE"=>"https://api.stlouisfed.org/fred/series/observations?series_id=PCEPILFE&file_type=json&aggregation_method=eop&output_type=1&sort_order=desc",
        "PPIACO"=>"https://api.stlouisfed.org/fred/series/observations?series_id=PPIACO&file_type=json&aggregation_method=eop&output_type=1&sort_order=desc"
    )

    TRADEINDEX=LittleDict{String,String}(
        "DTWEXBGS"=>"https://api.stlouisfed.org/fred/series/observations?series_id=DTWEXBGS&file_type=json&aggregation_method=eop&output_type=1&sort_order=desc",
        "DTWEXAFEGS"=>"https://api.stlouisfed.org/fred/series/observations?series_id=DTWEXAFEGS&file_type=json&aggregation_method=eop&output_type=1&sort_order=desc",
        "DTWEXEMEGS"=>"https://api.stlouisfed.org/fred/series/observations?series_id=DTWEXEMEGS&file_type=json&aggregation_method=eop&output_type=1&sort_order=desc"
    )


   
    final_fred_PRICES=LittleDict{String,DataFrame}()
    final_fred_TRADE=LittleDict{String,DataFrame}()

    for (k,url) in PRICES
        println("[+] Getting Data for ",k,"...")
        res=parseFREDData(url*"&observation_start="*string(OBSERVATION_START)*"&api_key="*FRED_API_KEY)
        sort!(res,[:Date])   
        merge!(final_fred_PRICES, Dict(k=>res))
    end


    for (k,url) in TRADEINDEX
        println("[+] Getting Data for ",k,"...")
        res=parseFREDData(url*"&observation_start="*string(OBSERVATION_START)*"&api_key="*FRED_API_KEY) 
        sort!(res,[:Date]) 
        merge!(final_fred_TRADE, Dict(k=>res))
    end

    XLSX.openxlsx(FILEPATH*"/Cyclical_Commodities/"*"CPI_PPI.xlsx",mode="w") do xf
        for (i,val) in final_fred_PRICES
            XLSX.addsheet!(xf,i)
            XLSX.writetable!(xf[i],val)
        end
    end

    XLSX.openxlsx(FILEPATH*"/Cyclical_Commodities/"*"TradeWeightedIndices.xlsx",mode="w") do xf
        for (i,val) in final_fred_TRADE
            XLSX.addsheet!(xf,i)
            XLSX.writetable!(xf[i],val)
        end
    end

end



main()