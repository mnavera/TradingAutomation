using HTTP
using DataFrames
using CSV
using Dates
using JSON

include("ENV.jl")
include("WebParsers.jl")

using .WebParsers

function main()
    println("-----------------ISM NMI US-------------------")
    #Get today's date and one year ago in epoch time
    CURRENT_EPOCH=Int(floor(Dates.datetime2unix(DateTime(Dates.now()))))
    ONEYR_EPOCH=CURRENT_EPOCH-31536000
    OBSERVATION_START=Date(Dates.unix2datetime(ONEYR_EPOCH))



    #Url format: https://data.nasdaq.com/api/v3/datasets/ISM/MAN_PMI.json?api_key=BMBTj1xMGpdNuwy96sXm"
    NMI_ISM=Dict(
        "NONMAN_NMI"=>"https://data.nasdaq.com/api/v3/datasets/ISM/NONMAN_NMI.json",
        "NONMAN_BUSACT"=>"https://data.nasdaq.com/api/v3/datasets/ISM/NONMAN_BUSACT.json",
        "NONMAN_EMPL"=>"https://data.nasdaq.com/api/v3/datasets/ISM/NONMAN_EMPL.json",
        "NONMAN_NEWORD"=>"https://data.nasdaq.com/api/v3/datasets/ISM/NONMAN_NEWORD.json",
        "NONMAN_DELIV"=>"https://data.nasdaq.com/api/v3/datasets/ISM/NONMAN_DELIV.json",
        "NONMAN_EXPORTS"=>"https://data.nasdaq.com/api/v3/datasets/ISM/NONMAN_EXPORTS.json",
        "NONMAN_INVENT"=>"https://data.nasdaq.com/api/v3/datasets/ISM/NONMAN_INVENT.json",
        "NONMAN_BACKLOG"=>"https://data.nasdaq.com/api/v3/datasets/ISM/NONMAN_BACKLOG.json",
        "NONMAN_IMPORTS"=>"https://data.nasdaq.com/api/v3/datasets/ISM/NONMAN_IMPORTS.json",
        "NONMAN_PRICES"=>"https://data.nasdaq.com/api/v3/datasets/ISM/NONMAN_PRICES.json",
        "NONMAN_INVSENT"=>"https://data.nasdaq.com/api/v3/datasets/ISM/NONMAN_INVSENT.json"
    )



    for (k,url) in NMI_ISM
        print("[+] Getting Data for ",k,"...")
        #set filename
        fname=FILEPATH*"/ISM_NMI_US/"*k*".csv"
        try
            df=parseNASDAQData(url*"?start_date="*string(OBSERVATION_START))
            if k == "NONMAN_NMI"
                CSV.write(fname,df[!, [:Date,:Index]])
            else
                #println(propertynames(df))
                CSV.write(fname,df[!, [:Date,Symbol("Diffusion Index")]])
            end
            #println(df)
            println("SUCCESS")
        catch e
            println(e)
        end

        #adjust data dimensions
    end
end

main()