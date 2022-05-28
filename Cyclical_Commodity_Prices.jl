using OrderedCollections
using DataFrames
using XLSX
using Dates
using HTTP
using CSV

include("ENV.jl")
include("Stocks.jl")
include("WebParsers.jl")

using .Stocks, .WebParsers


function createCommodityFile(fname::String,data::LittleDict)
    XLSX.openxlsx(fname,mode="w") do xf
        for (i,val) in data
            XLSX.addsheet!(xf,i)
            XLSX.writetable!(xf[i],val)
            #println(last(i.daily,10))
        end
    end
end

function main()

    println("-----------------Cyclical Commodities-------------------")
    SPREADS=[:COMEX_SHFE,:LME_SHFE,:CME_DCE]
    DATA_BLOB=LittleDict{String,DataFrame}()


    #Get today's date and one year ago in epoch time
    CURRENT_EPOCH=Int(floor(Dates.datetime2unix(DateTime(Dates.now()))))
    ONEYR_EPOCH=CURRENT_EPOCH-(31536000)
    TENYR_EPOCH=CURRENT_EPOCH-(31536000*10)


    OBSERVATION_START=Date(Dates.unix2datetime(ONEYR_EPOCH))
    TENYR_EPOCH=Date(Dates.unix2datetime(TENYR_EPOCH))

    YFINANCE_URL="https://query1.finance.yahoo.com/v7/finance/download/USDCNY%3DX"
    YFINANCE_URL_PARAMS="events=history&includeAdjustedClose=true"*"&period1=-1325635200&period2="*string(CURRENT_EPOCH)*"&interval=1d"
    
    INVESTINGCOM_URL="https://www.investing.com/instruments/HistoricalDataAjax"
    COMMODITIES=LittleDict{String,Int64}(
        "WTI Crude Oil"=>8849,
        "Brent Oil"=>8833,
        "Copper(COMEX)"=>8831,
        "Copper(LME)"=>959211,
        "Copper(SHFE)"=>996725,
        "Lumber"=>959198,
        "Iron Ore(CME)"=>961729,
        "Iron Ore(DCE)"=>961741
    )
    POST_PARAMS=Dict{}(
        "st_date"=>Dates.format(TENYR_EPOCH,"mm/dd/YYYY"),
        "end_date"=>Dates.format(Dates.now(),"mm/dd/YYYY"),
        "interval_sec"=>"Daily",
        "sort_col"=>"date",
        "sort_ord"=>"ASC",
        "action"=>"historical_data",
        "curr_id"=>8849
    )


    println("[+] Getting Data for CNY_USD...")
    #Get CNY and USD spread price
    CNY_USD=Stock("CNY_USD")
    populate(CNY_USD,parseYFData(YFINANCE_URL,YFINANCE_URL_PARAMS),'d')
    cleanStockData(CNY_USD)

    #fill out Adj Close column so we have values for the spreads
    for (i,val) in enumerate(CNY_USD.daily[:,Symbol("Adj Close")])
        if ismissing(val)
            CNY_USD.daily[i,Symbol("Adj Close")]=CNY_USD.daily[i-1,Symbol("Adj Close")]
        end
    end


    #Get Commodity prices
    for (i,val) in COMMODITIES
        println("[+] Getting Data for ",i,"...")
        POST_PARAMS["curr_id"]=val
        res=parseInvestingData(INVESTINGCOM_URL,POST_PARAMS)
        sort!(res)
        #Merge all into one dataframe
        merge!(DATA_BLOB, Dict(i=>res))
    end
    
    #Set spread dates
    println("[+] Getting Data for Spreads")
    COMEX_SHFE=leftjoin(DATA_BLOB["Copper(COMEX)"][:,[:Date]],CNY_USD.daily[:,[:Date,Symbol("Adj Close")]],on=:Date)
    LME_SHFE=leftjoin(DATA_BLOB["Copper(LME)"][:,[:Date]],CNY_USD.daily[:,[:Date,Symbol("Adj Close")]],on=:Date)
    CME_DCE=leftjoin(DATA_BLOB["Iron Ore(DCE)"][:,[:Date]],CNY_USD.daily[:,[:Date,Symbol("Adj Close")]],on=:Date)

    

    #fill in missing values and merge to data blob
    for i in [COMEX_SHFE,LME_SHFE,CME_DCE]
        sort!(i)
        for (j,val) in enumerate(i[:,2])
            if ismissing(val)
                i[j,2] = i[j-1,2]
            end
        end
    end
    #merge the spread data in with the rest of our data
    merge!(DATA_BLOB,LittleDict("COMEX_SHFE"=>COMEX_SHFE,"LME_SHFE"=>LME_SHFE,"CME_DCE"=>CME_DCE),LittleDict("CNY_USD"=>CNY_USD.daily))

    println("\n\n[+] Creating File...")
    fname=FILEPATH*"/Cyclical_Commodities/Cyclical_Commodity_PriceData.xlsx"
    createCommodityFile(fname,DATA_BLOB)
    println("File at ",fname)


end

main()