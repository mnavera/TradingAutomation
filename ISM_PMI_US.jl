using HTTP
using DataFrames
using CSV
using Dates
using JSON

include("ENV.jl")
include("WebParsers.jl")

using .WebParsers

function main()
    println("-----------------ISM PMI US-------------------")
    #Get today's date and one year ago in epoch time
    CURRENT_EPOCH=Int(floor(Dates.datetime2unix(DateTime(Dates.now()))))
    ONEYR_EPOCH=CURRENT_EPOCH-31536000
    OBSERVATION_START=Date(Dates.unix2datetime(ONEYR_EPOCH))



    #Url format: https://data.nasdaq.com/api/v3/datasets/ISM/MAN_PMI.json?api_key=BMBTj1xMGpdNuwy96sXm"
    PMI_ISM=Dict(
        "MAN_PMI"=>"https://data.nasdaq.com/api/v3/datasets/ISM/MAN_PMI.json",
        "MAN_DELIV"=>"https://data.nasdaq.com/api/v3/datasets/ISM/MAN_DELIV.json",
        "MAN_CUSTINV"=>"https://data.nasdaq.com/api/v3/datasets/ISM/MAN_CUSTINV.json",
        "MAN_PRICES"=>"https://data.nasdaq.com/api/v3/datasets/ISM/MAN_PRICES.json",
        "MAN_EMPL"=>"https://data.nasdaq.com/api/v3/datasets/ISM/MAN_EMPL.json",
        "MAN_NEWORDERS"=>"https://data.nasdaq.com/api/v3/datasets/ISM/MAN_NEWORDERS.json",
        "MAN_BACKLOG"=>"https://data.nasdaq.com/api/v3/datasets/ISM/MAN_BACKLOG.json",
        "MAN_PROD"=>"https://data.nasdaq.com/api/v3/datasets/ISM/MAN_PROD.json",
        "MAN_EXPORTS"=>"https://data.nasdaq.com/api/v3/datasets/ISM/MAN_EXPORTS.json",
        "MAN_INVENT"=>"https://data.nasdaq.com/api/v3/datasets/ISM/MAN_INVENT.json",
        "MAN_IMPORTS"=>"https://data.nasdaq.com/api/v3/datasets/ISM/MAN_IMPORTS.json"
    )



    #for (k,url) in PMI_ISM
    #    r=HTTP.request("GET", url*"?api_key="*NASDAQ_API_KEY*"&start_date="*string(OBSERVATION_START); verbose=1)
    #    println(k,": ", r.status)
    #end




    for (k,url) in PMI_ISM
        print("[+] Getting Data for ",k,"...")
        #set filename
        fname=FILEPATH*"/ISM_PMI_US/"*k*".csv"
        try
            df=parseNASDAQData(url*"?start_date="*string(OBSERVATION_START))
            if k == "MAN_PMI"
                CSV.write(fname,df[!, [:Date,:PMI]])
            else
                CSV.write(fname,df[!, [:Date,:Index]])
            end
            println("SUCCESS")
        catch e
            println(e)
        end
    end
end

main()