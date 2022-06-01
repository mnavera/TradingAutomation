using OrderedCollections
using DataFrames
using XLSX
using Dates
using HTTP
using CSV
using LibPQ
using Decimals

include("ENV.jl")
include("Stocks.jl")
include("WebParsers.jl")
include("DBInterface.jl")

using .Stocks, .WebParsers, .DBInterface


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

    TABLEMAPS=Dict(
        "WTI Crude Oil"=>"wti_crude_oil",
        "Brent Oil"=>"brent_oil",
        "Copper(COMEX)"=>"copper_comex",
        "Copper(LME)"=>"copper_lme",
        "Copper(SHFE)"=>"copper_shfe",
        "Lumber"=>"lumber",
        "Iron Ore(CME)"=>"iron_cme",
        "Iron Ore(DCE)"=>"iron_dce",
        "CNY_USD"=>"cny_usd"
    )

    #Setup SQL Connection
    commdb="trading_db"
    schema="trading_comms_prices"

    #testing values
    #=commdb="trading_db_test"
    schema="trading_comms_prices_t"=#

    #connect to DB
    conn=DBInterface.openDB(commdb)
    daysToUpdate=10




    println("[+] Getting Data for CNY_USD...")
    #Get CNY and USD spread price
    CNY_USD=Stock("CNY_USD")
    populate(CNY_USD,parseYFData(YFINANCE_URL,YFINANCE_URL_PARAMS),'d')
    cleanStockData(CNY_USD)


    #fill out Adj Close column so we have values for the spreads
    tabname = TABLEMAPS["CNY_USD"]
    for (i,val) in enumerate(CNY_USD.daily[:,Symbol("Adj Close")])
        if ismissing(val)
            CNY_USD.daily[i,Symbol("Adj Close")]=CNY_USD.daily[i-1,Symbol("Adj Close")]

        end
    end

    #Check if database is up-to-date, and only enter the missing dates
    updated,ind=DBInterface.checkIfUpdated(conn,schema,tabname,CNY_USD.daily)
    !updated ? DBInterface.insertToDB(conn,schema,tabname,CNY_USD.daily,ind,"Y") : println("Nothing to insert")

    #get data from SQL database
    indb_df=DBInterface.checkEntry(conn,schema,tabname,17)[:,[Symbol(tabname*"_date"),Symbol(tabname*"_price")]]

    #variable to pass into function to keep our base df untouched
    CNY_USD_copy=sort(CNY_USD.daily[:,[:Date,Symbol("Adj Close")]])
    
    #Compare and correct any anomalies over the specified days
    DBInterface.checkAndCorrectDB(conn,schema,tabname,indb_df,CNY_USD_copy,"Y",10)


    

    #Get Commodity prices
    for (i,val) in COMMODITIES
        println("[+] Getting Data for ",i,"...")
        POST_PARAMS["curr_id"]=val
        res=parseInvestingData(INVESTINGCOM_URL,POST_PARAMS)
        sort!(res)

        tabname = TABLEMAPS[i] #get db table names

        #get Daily and weekly returns and append to Investing.com data
        for i in (1,5)
            returns_df=Stocks.calculateReturns(res[:,[:Date,:Prices]], ran=i)
            #println(returns_df)

            #sanitize data to prepare for inserting
            mapcols!(col -> replace(col,missing=>"NULL"),returns_df)

            temp_df=leftjoin(res,returns_df, on=:Date)
            #println(temp_df)
           
            i == 1 ? res.Daily=temp_df[!,:Returns] : res.Weekly=temp_df[!,:Returns]

        end


        #Check if we need to update the table in the database
        updated,ind=DBInterface.checkIfUpdated(conn,schema,tabname,res)
        !updated ? DBInterface.insertToDB(conn,schema,tabname,res[!,[:Date,:Prices,:Daily,:Weekly]],ind,"I") : println("Nothing to insert")






        #Check if our data entries are correct for the past 10 entries. we get 17 because in case we need to recompute returns values
        indb_df=DBInterface.checkEntry(conn,schema,tabname,17)[:,[Symbol(tabname*"_date"),Symbol(tabname*"_price")]]

        res_copy=sort(res)#variable to pass into function to keep our base df untouched
        DBInterface.checkAndCorrectDB(conn,schema,tabname,indb_df,res_copy,"I",10)
        
        #Merge all into one dataframe for writing to file
        merge!(DATA_BLOB, Dict(i=>res))
    end
    









    #Set spread dates
    println("[+] Getting Data for Spreads")
    COMEX_SHFE=leftjoin(DATA_BLOB["Copper(COMEX)"][:,[:Date]],CNY_USD.daily[:,[:Date,Symbol("Adj Close")]],on=:Date)
    LME_SHFE=leftjoin(DATA_BLOB["Copper(LME)"][:,[:Date]],CNY_USD.daily[:,[:Date,Symbol("Adj Close")]],on=:Date)
    CME_DCE=leftjoin(DATA_BLOB["Iron Ore(DCE)"][:,[:Date]],CNY_USD.daily[:,[:Date,Symbol("Adj Close")]],on=:Date)

    










    #fill in missing values
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





    #make sure to close SQL connection
    DBInterface.closeDB(conn)

end

main()

