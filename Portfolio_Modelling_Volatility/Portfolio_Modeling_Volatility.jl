using HTTP
using DataFrames
using CSV
using Dates
using OrderedCollections
using ArgParse
using XLSX


include("../ENV.jl")
include("../Stocks.jl")
include("../WebParsers.jl")

using .Stocks, .WebParsers

#=General Flow:
1. Get user-inputted stock tickers
2. Download stock ticker data(weekly/daily) from YFINANCE. Download S&P500 data as well for beta calculations
    2.1 Store each stock ticker data(inc. S&P500) in its DataFrame
3/4. get lengths of each set of data and put lengths into Dict with ticker symbol as key
    3/4.1 Find shortest length. Set that as the Date range for Portfolio Volatility calculation
3/4. Get Beta calculations for each ticker. Priority list goes as follows:
    3Y Weekly, 2Y Weekly, 180D Daily, 90D Daily
5. Create copies of template files(Portfolio Modeling, Portfolio Volatility, Beta)
6.For Portfolio Returns Modeling, use Daily returns. for Portfolio Volatility, use Weekly returns. Beta calculations are as in step 3/4
7. Enter Portfolio Modeling Data as new Sheets in Excel file, with rows(Date,Open,High,Low,Close,Adj Close). One sheet per stock to be modelled.
    7.1 Portfolio modeling data to be used will be mainly the more recent ones. 
    7.2 At Sheet 'Portfolio CW Perf', input ticker symbols starting from column B1:S1. 
        7.2.1 Long/Short indication to be 1/-1 starting from B3:S3
        7.2.2 Date values starting from B9 downwards, Portfolio Index(in USD) at U9.
    7.2 At Sheet 'Portfolio NRB Perf', input ticker symbols starting from column B1:S1.
        7.3.1 Long/Short indication to be 1/-1 starting from B5:S5
        7.3.2 # Shares would be the desired exposure divided by initial stock price of data set.   
8. Enter Portfolio Volatility Data in 'Asset Returns' Sheet, with input being the trimmed DataFrame from step 3/4.
    8.1 At 'Portfolio Volatility' Sheet, input ticker symbols from B9:B33, and B49:B73.
        8.1.2 $ Net Allocation at AB9:AB33
        8.2.2 Long/Short indication will be 1/-1 at AB49:AB73
    8.2 At 'Asset Prices' Sheet, DataFrame data will be inputted from C2
    8.3 At 'Asset Returns' Sheet, ticker data to be inputed from D1 onwards, horizontally
        8.3.3 Have to figure out a way to populate formula cells up to the size of 'Asset Prices'

9. At Calculating Beta, populate 'Portfolio Beta' sheet
    9.1 Company name at A3:A14, A18:A29
    9.2 Company ticker at B3:B14, B18:B29
    9.3 Beta at C3:C14, C18:C29
    9.4 Current Price at D3:D14, D18:D29
=#

function getInputs()
end


function parse_commandline()
    s = ArgParseSettings()

    @add_arg_table s begin
        "--tickers", "-t"
            help = "Tickers, separated by commas"
            required = true
        "--positions", "-p"
            help = "Positions of tickers, in order"
            required = true
    end

    #argstring=values(parse_args(s))

    return parse_args(s)
end


#prepare data for putting into Portfolio volatility spreadsheet
function buildPortVolData(df_data,len::Int64)
    portvol_df=DataFrame()
    #=portret_df=DataFrame()
    for (i,val) in df_data
        temp_df=sort(val,rev=true)
        vol_df=temp_df[1:len,[:Date, Symbol("Adj Close")]]
        ret_df=temp_df[1:len,[:Date,:Returns]]=#

    for (i,val) in df_data
        val_df=last(val.weekly[:,[:Date, Symbol("Adj Close")]],len)
        rename!(val_df,Symbol("Adj Close")=>Symbol(i))
        if isempty(portvol_df)
            portvol_df=val_df
        else
            portvol_df=leftjoin(portvol_df,val_df, on=:Date)
        end
    end


    return portvol_df
end





#Create Excel files
function createBetaFile(fname::String,data::LittleDict)
    betaData=DataFrame([:Ticker=>String[],:Beta=>Float64[],:Price=>Any[],:Position=>[]])
    for i in values(data)
        push!(betaData,[i.name,i.beta,last(i.daily[!,Symbol("Adj Close")]),i.position])
    end
    XLSX.openxlsx(fname,mode="w") do xf
        sheet=xf[1]
        #println(XLSX.sheetnames(sheet))
        XLSX.writetable!(sheet,betaData)
    end
end

function createVolatilityFile(fname::String,data::LittleDict)
    weeklyDataLengths=Int64[]
    for i in values(data)
        append!(weeklyDataLengths,Stocks.length(i,'w'))
    end
    portvol_length=minimum(weeklyDataLengths)#Get shortest data set
    portvol=buildPortVolData(data,portvol_length)
    XLSX.openxlsx(fname,mode="w") do xf
        sheet=xf[1]
        #println(XLSX.sheetnames(sheet))
        XLSX.writetable!(sheet,portvol)
    end
end

function createModellingFile(fname::String,data::LittleDict)
    XLSX.openxlsx(fname,mode="w") do xf
        for i in values(data)
            XLSX.addsheet!(xf,i.name)
            XLSX.writetable!(xf[i.name],i.daily)
            #println(last(i.daily,10))
        end
    end
end




function main() 
    #Values for testing
    #STOCKS=["USDCNY%3DX","FLR"]
    STOCKDATA=LittleDict{String,Int64}(
        "WING"=>1,
        "FLR"=>-1,
        "MAN"=>1
    )

    #parse command line arguments into 
    parsed_args = parse_commandline()
    tickers=parsed_args["tickers"]
    positions=parsed_args["positions"]

    #Our tickers
    STOCKDATA=split(tickers,",")

    #Long/short, 1/-a respectively for each stock
    parr=split(positions,",")
    parr=tryparse.(Int64,parr)
    #STOCKS=["LVS","AAPL","WING","FLR","MAN","BBY","COKE","HGV","GCI","TALK"]


    #Location for upload
    fpath=FILEPATH*"/Portfolio_Modelling_Volatility/"
    BETA_FILE=fpath*"Beta.xlsx"
    VOl_FILE=fpath*"Portfolio_Volatility_Correlation.xlsx"
    MODEL_FILE=fpath*"Portfolio_Modelling.xlsx"

    #Get today's date and one year ago in epoch time
    CURRENT_EPOCH=Int(floor(Dates.datetime2unix(DateTime(Dates.now()))))
    ONEYR_EPOCH=CURRENT_EPOCH-31536000
    TWOYR_EPOCH=CURRENT_EPOCH-(31536000*2)
    THREEYR_EPOCH=CURRENT_EPOCH-(31536000*3)
    SIXMO_EPOCH=CURRENT_EPOCH-(31536000/2)
    THREEMO_EPOCH=CURRENT_EPOCH-(31536000/4)


    OBSERVATION_START=Date(Dates.unix2datetime(ONEYR_EPOCH))
    THREEMO_DATE=Date(Dates.unix2datetime(THREEMO_EPOCH))
    SIXMO_DATE=Date(Dates.unix2datetime(SIXMO_EPOCH))
    TWOYR_DATE=Date(Dates.unix2datetime(TWOYR_EPOCH))
    THREEYR_DATE=Date(Dates.unix2datetime(THREEYR_EPOCH))

    #Date Interval Indicators
    Interval=Dict(
        'd'=>"1d",
        'w'=>"1wk",
        'm'=>"1mo"
    )

    YFINANCE_URL="https://query1.finance.yahoo.com/v7/finance/download/"
    YFINANCE_URL_PARAMS="events=history&includeAdjustedClose=true"*"&period1=-1325635200&period2="*string(CURRENT_EPOCH)*"&interval="


    #Dictionary where all our data will be stored
    DATA_BLOB=LittleDict{String,Stock}()


    SP_500=Stock("SP_500")

    #Get CSV Data from YFINANCE
    println("\n","---Getting YFINANCE Data---", "\n")


    #Get Data for S&P500
    println("[+] Getting Data for S&P500...")
    @sync begin
        for i in ['m','w','d']
            @async begin
                populate(SP_500,parseYFData(YFINANCE_URL*"%5EGSPC","events=history&includeAdjustedClose=true&period1=-1325635200&period2=1650129126&interval="*Interval[i]),i,1)
            end
        end
    end

    


  
    #Calculate S&P500 returns
    for i in ['m','w','d']
        Stocks.dropmissing!(SP_500,i)
        calculateReturns!(SP_500,i)
    end

    #Populate and arrange ticker data
    @sync begin
        for (pos,i) in enumerate(STOCKDATA)
            @async begin
                s=Stock(i)
                println("[+] Getting Data for ",s.name,"...")
                @sync begin
                    for j in ['m','w','d']
                        @async begin
                            populate(s,parseYFData(YFINANCE_URL*i, YFINANCE_URL_PARAMS*Interval[j]),j,parr[pos])
                            #clean up data for processing
                            cleanStockData(s)
                
                            calculateReturns!(s,j)
                        end
                    end
                end
                s.beta=collect(values(calculateBeta(s,SP_500)))[1]
        
                #merge all data within one Dict
                merge!(DATA_BLOB, LittleDict(i=>s))                  
                
  
            end

        end
    end


    #create corresponding files
    println("[+] Getting Beta File...")
    createBetaFile(BETA_FILE,DATA_BLOB)
    println("[+] Getting Volatility File...")
    createVolatilityFile(VOl_FILE,DATA_BLOB)
    println("[+] Getting Modelling File...")
    createModellingFile(MODEL_FILE,DATA_BLOB)
    
    println("DONE!")

end

main()


