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
    #Get today's date and one year ago in epoch time
    println("-----------------Coincident Indicators US-------------------")
    CURRENT_EPOCH=Int(floor(Dates.datetime2unix(DateTime(Dates.now()))))
    TWOYR_EPOCH=CURRENT_EPOCH-(31536000 * 2)
    OBSERVATION_START=Date(Dates.unix2datetime(TWOYR_EPOCH))


    
    #Location for upload
    fpath=FILEPATH*"/Coincident_Indicators/"
    
    #Delete old CSVs in directory before adding in new ones
    #rm(FILEPATH, recursive=true, force=true)
    #mkpath(FILEPATH)


    DURGOODS=LittleDict{String,String}(
        "DGORDER"=>"https://api.stlouisfed.org/fred/series/observations?series_id=DGORDER&file_type=json&aggregation_method=eop&output_type=1&sort_order=desc",
        "ADXTNO"=>"https://api.stlouisfed.org/fred/series/observations?series_id=ADXTNO&file_type=json&aggregation_method=eop&output_type=1&sort_order=desc",
        "ADXDNO"=>"https://api.stlouisfed.org/fred/series/observations?series_id=ADXDNO&file_type=json&aggregation_method=eop&output_type=1&sort_order=desc",
        "A34SNO"=>"https://api.stlouisfed.org/fred/series/observations?series_id=A34SNO&file_type=json&aggregation_method=eop&output_type=1&sort_order=desc",
        "A35SNO"=>"https://api.stlouisfed.org/fred/series/observations?series_id=A35SNO&file_type=json&aggregation_method=eop&output_type=1&sort_order=desc",
        "A33SNO"=>"https://api.stlouisfed.org/fred/series/observations?series_id=A33SNO&file_type=json&aggregation_method=eop&output_type=1&sort_order=desc",
        "A32SNO"=>"https://api.stlouisfed.org/fred/series/observations?series_id=A32SNO&file_type=json&aggregation_method=eop&output_type=1&sort_order=desc",
        "A31SNO"=>"https://api.stlouisfed.org/fred/series/observations?series_id=A31SNO&file_type=json&aggregation_method=eop&output_type=1&sort_order=desc",
        "A36SNO"=>"https://api.stlouisfed.org/fred/series/observations?series_id=A36SNO&file_type=json&aggregation_method=eop&output_type=1&sort_order=desc",
        "A37SNO"=>"https://api.stlouisfed.org/fred/series/observations?series_id=A37SNO&file_type=json&aggregation_method=eop&output_type=1&sort_order=desc"
    )

    EMPL=LittleDict{String,String}(
        "UNRATE"=>"https://api.stlouisfed.org/fred/series/observations?series_id=UNRATE&file_type=json&aggregation_method=eop&output_type=1&sort_order=desc",
        "PAYEMS"=>"https://api.stlouisfed.org/fred/series/observations?series_id=PAYEMS&file_type=json&aggregation_method=eop&output_type=1&sort_order=desc",
        "USGOVT"=>"https://api.stlouisfed.org/fred/series/observations?series_id=USGOVT&file_type=json&aggregation_method=eop&output_type=1&sort_order=desc",
        "USPRIV"=>"https://api.stlouisfed.org/fred/series/observations?series_id=USPRIV&file_type=json&aggregation_method=eop&output_type=1&sort_order=desc",
        "USGOOD"=>"https://api.stlouisfed.org/fred/series/observations?series_id=USGOOD&file_type=json&aggregation_method=eop&output_type=1&sort_order=desc",
        "SRVPRD"=>"https://api.stlouisfed.org/fred/series/observations?series_id=SRVPRD&file_type=json&aggregation_method=eop&output_type=1&sort_order=desc",
        "USCONS"=>"https://api.stlouisfed.org/fred/series/observations?series_id=USCONS&file_type=json&aggregation_method=eop&output_type=1&sort_order=desc",
        "MANEMP"=>"https://api.stlouisfed.org/fred/series/observations?series_id=MANEMP&file_type=json&aggregation_method=eop&output_type=1&sort_order=desc",
        "USTPU"=>"https://api.stlouisfed.org/fred/series/observations?series_id=USTPU&file_type=json&aggregation_method=eop&output_type=1&sort_order=desc",
        "USFIRE"=>"https://api.stlouisfed.org/fred/series/observations?series_id=USFIRE&file_type=json&aggregation_method=eop&output_type=1&sort_order=desc",
        "USPBS"=>"https://api.stlouisfed.org/fred/series/observations?series_id=USPBS&file_type=json&aggregation_method=eop&output_type=1&sort_order=desc"

    )

    IND_PROD=LittleDict{String,String}(
        "INDPRO"=>"https://api.stlouisfed.org/fred/series/observations?series_id=INDPRO&file_type=json&aggregation_method=eop&output_type=1&sort_order=desc",
        "IPMAN"=>"https://api.stlouisfed.org/fred/series/observations?series_id=IPMAN&file_type=json&aggregation_method=eop&output_type=1&sort_order=desc",
        "IPG311A2S"=>"https://api.stlouisfed.org/fred/series/observations?series_id=IPG311A2S&file_type=json&aggregation_method=eop&output_type=1&sort_order=desc",
        "IPG313A4S"=>"https://api.stlouisfed.org/fred/series/observations?series_id=IPG313A4S&file_type=json&aggregation_method=eop&output_type=1&sort_order=desc",
        "IPG325S"=>"https://api.stlouisfed.org/fred/series/observations?series_id=IPG325S&file_type=json&aggregation_method=eop&output_type=1&sort_order=desc",
        "IPG326S"=>"https://api.stlouisfed.org/fred/series/observations?series_id=IPG326S&file_type=json&aggregation_method=eop&output_type=1&sort_order=desc",
        "IPG315A6S"=>"https://api.stlouisfed.org/fred/series/observations?series_id=IPG315A6S&file_type=json&aggregation_method=eop&output_type=1&sort_order=desc",
        "IPG321S"=>"https://api.stlouisfed.org/fred/series/observations?series_id=IPG321S&file_type=json&aggregation_method=eop&output_type=1&sort_order=desc",
        "IPG322S"=>"https://api.stlouisfed.org/fred/series/observations?series_id=IPG322S&file_type=json&aggregation_method=eop&output_type=1&sort_order=desc",
        "IPG323S"=>"https://api.stlouisfed.org/fred/series/observations?series_id=IPG323S&file_type=json&aggregation_method=eop&output_type=1&sort_order=desc",
        "IPG324S"=>"https://api.stlouisfed.org/fred/series/observations?series_id=IPG324S&file_type=json&aggregation_method=eop&output_type=1&sort_order=desc",
        "IPG327S"=>"https://api.stlouisfed.org/fred/series/observations?series_id=IPG327S&file_type=json&aggregation_method=eop&output_type=1&sort_order=desc",
        "IPG331S"=>"https://api.stlouisfed.org/fred/series/observations?series_id=IPG331S&file_type=json&aggregation_method=eop&output_type=1&sort_order=desc",
        "IPG332S"=>"https://api.stlouisfed.org/fred/series/observations?series_id=IPG332S&file_type=json&aggregation_method=eop&output_type=1&sort_order=desc",
        "IPG333S"=>"https://api.stlouisfed.org/fred/series/observations?series_id=IPG333S&file_type=json&aggregation_method=eop&output_type=1&sort_order=desc",
        "IPG334S"=>"https://api.stlouisfed.org/fred/series/observations?series_id=IPG334S&file_type=json&aggregation_method=eop&output_type=1&sort_order=desc",
        "IPG335S"=>"https://api.stlouisfed.org/fred/series/observations?series_id=IPG335S&file_type=json&aggregation_method=eop&output_type=1&sort_order=desc",
        "IPG3361T3S"=>"https://api.stlouisfed.org/fred/series/observations?series_id=IPG3361T3S&file_type=json&aggregation_method=eop&output_type=1&sort_order=desc",
        "IPG3364T9N"=>"https://api.stlouisfed.org/fred/series/observations?series_id=IPG3364T9N&file_type=json&aggregation_method=eop&output_type=1&sort_order=desc",
        "IPG337S"=>"https://api.stlouisfed.org/fred/series/observations?series_id=IPG337S&file_type=json&aggregation_method=eop&output_type=1&sort_order=desc",
        "IPG339S"=>"https://api.stlouisfed.org/fred/series/observations?series_id=IPG339S&file_type=json&aggregation_method=eop&output_type=1&sort_order=desc",
    )

    JOBLESS=LittleDict{String,String}(
        "ICSA"=>"https://api.stlouisfed.org/fred/series/observations?series_id=ICSA&file_type=json&aggregation_method=eop&output_type=1&sort_order=desc",
        "CCSA"=>"https://api.stlouisfed.org/fred/series/observations?series_id=CCSA&file_type=json&aggregation_method=eop&output_type=1&sort_order=desc"
    )


   


    #declare Dataframe funnels for data
    df_durgoods=DataFrame()
    df_empl=DataFrame()
    df_prod=DataFrame()



    #Durable Goods Shipment
    println("\n","---Getting Durable Goods---", "\n")
    for (k,url) in DURGOODS
        println("[+] Getting Data for ",k,"...")
        res=parseFREDData(url*"&observation_start="*string(OBSERVATION_START)*"&api_key="*FRED_API_KEY)
        #res=parseFREDData(url*"&api_key="*FRED_API_KEY)
        date_cols="Date_"*k
        value_cols="Value_"*k

        #res[!, :Date] = Date.(res[:,:Date],"m d y")

        df_durgoods[!,date_cols] = last(res.Date,12)
        df_durgoods[!,value_cols] = last(res.Value,12)
        #CSV.write(FILEPATH*k*"_"*string(Dates.today())*".csv",res)
    end

    CSV.write(fpath*"Durable_Goods"*"_"*string(Dates.today())*".csv",df_durgoods)






    #Employment Situation Report
    println("\n","---Getting Employment Situation---", "\n")
    for (k,url) in EMPL
        println("[+] Getting Data for ",k,"...")
        res=parseFREDData(url*"&observation_start="*string(OBSERVATION_START)*"&api_key="*FRED_API_KEY)
        #res=parseFREDData(url*"&api_key="*FRED_API_KEY)
        date_cols="Date_"*k
        value_cols="Value_"*k

        #res[!, :Date] = Date.(res[:,:Date],"m d y")

        df_empl[!,date_cols] = last(res.Date,12)
        df_empl[!,value_cols] = last(res.Value,12)
        #CSV.write(FILEPATH*k*"_"*string(Dates.today())*".csv",res)
    end

    CSV.write(fpath*"Employment_Situation"*"_"*string(Dates.today())*".csv",df_empl)

    



    #Industrial Production
    println("\n","---Getting Industrial Production---", "\n")
    for (k,url) in IND_PROD
        println("[+] Getting Data for ",k,"...")
        res=parseFREDData(url*"&observation_start="*string(OBSERVATION_START)*"&api_key="*FRED_API_KEY)
        #res=parseFREDData(url*"&api_key="*FRED_API_KEY)
        date_cols="Date_"*k
        value_cols="Value_"*k

        #res[!, :Date] = Date.(res[:,:Date],"m d y")

        df_prod[!,date_cols] = last(res.Date,12)
        df_prod[!,value_cols] = last(res.Value,12)
        #CSV.write(FILEPATH*k*"_"*string(Dates.today())*".csv",res)
    end

    CSV.write(fpath*"Industrial_Production"*"_"*string(Dates.today())*".csv",df_prod)






    #Jobless Claims

    println("\n","---Getting Jobless Claims---", "\n")
    for (k,url) in JOBLESS
        println("[+] Getting Data for ",k,"...")
        #res=parseFREDData(url*"&observation_start="*string(OBSERVATION_START)*"&api_key="*FRED_API_KEY)
        res=parseFREDData(url*"&api_key="*FRED_API_KEY)
 

        #res[!, :Date] = Date.(res[:,:Date],"m d y")

        #df_jobless[!,date_cols] = res.Date
        #df_jobless[!,value_cols] = res.Value
        CSV.write(fpath*k*"_JOBLESS_"*string(Dates.today())*".csv",res)
    end


    

end



main()