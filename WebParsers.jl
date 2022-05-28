module WebParsers
using HTTP,JSON,DataFrames,Gumbo,Cascadia,CSV,Dates

include("ENV.jl")

export parseFREDData,parseNASDAQData,parseYFData,parseInvestingData

function parseFREDData(fred_url)
    r=HTTP.request("GET", fred_url; verbose=0)
    j = JSON.parse(String(r.body))["observations"]
    df = DataFrame(Date=Any[], Value=Any[])

    #Get date and value fields from JSON blob
    map(x->push!(df,[x["date"],x["value"]]),j)
    
    #convert string to date type
    map!(x->x=Date(x,dateformat"y-m-d"),df.Date,df.Date)

    #convert string to floating point
    df.Value = tryparse.(Float64,df[!,:Value])
    
    #clean Values data
    #df[df.Value.==nothing,:Value] .= missing

    df.Value = replace(df.Value,nothing=> missing)



    #sort date by oldest to newest
    return sort!(df,[:Date],rev=true) 
end

function parseNASDAQData(nasdaq_url)

    r=HTTP.request("GET",nasdaq_url*"&api_key="*NASDAQ_API_KEY*"&order=asc")
    j = JSON.parse(String(r.body))
    colnames=Symbol.(j["dataset"]["column_names"])
    data=j["dataset"]["data"]

    #adjust data dimensions
    f=hcat(data...)
    g=permutedims(f, [2,1])
    df=DataFrame(g, colnames)

    return df
end

function parseYFData(url,q="")
    if(q=="")
        r=HTTP.request("GET", url, verbose=0)
    else
        r=HTTP.request("GET", url, query=q; verbose=0)
    end

    working_df=DataFrame(CSV.File(r.body))
     select!(working_df, Not(:Volume))
    
    for (j,c) in enumerate(eachcol(working_df))#enumerate over each column
        if any(isnothing,working_df[:,j])#check if our column has nothing values, replace with missing
            working_df[!,j] = replace(working_df[!,j],nothing=> missing)
        end

        if !(typeof(c) == Vector{Dates.Date} || typeof(c) == Vector{Float64} || typeof(c) == Vector{Union{Missing, Float64}})
            working_df[!,j] = tryparse.(Float64,c)
        end

        if any(isnothing,working_df[:,j])#check if our column has nothing values, replace with missing
            working_df[!,j] = replace(working_df[!,j],nothing=> missing)
        end
        
    end

    return working_df
end



function parseInvestingData(url,params::Dict)
    df=DataFrame()
    assetDates=Date[]
    prices=Float64[]
    openPrices=Float64[]
    highPrices=Float64[]
    lowPrices=Float64[]
    volumes=Int64[]
    pChange=Float64[]



    r=HTTP.request("POST",url,["X-Requested-With"=>"XMLHttpRequest","Content-Type"=>"application/x-www-form-urlencoded"],HTTP.URIs.escapeuri(params); verbose=0)
    res=parsehtml(String(r.body))
    #look for the table
    blob=eachmatch(sel"tbody",res.root)[1]
    rows=eachmatch(sel"tr",blob)

    #iterate through each row
    for i in rows
        record=eachmatch(sel"td",i)

        for i in [1,2,3,4,5,6,7]

            #parse data from html
            if i == 1 || i == 6
                res=tryparse(Int64,replace(record[i].attributes["data-real-value"],","=>""))
            elseif i == 7
                res=tryparse(Float64,record[i][1].text[1:end-1])
            else 
                res=tryparse(Float64,replace(record[i].attributes["data-real-value"],","=>""))
            end

            if isnothing(res)
                res=missing
            end

            
            if i == 1 #populate Date field
                push!(assetDates,Date(Dates.unix2datetime(res)))
            elseif i == 2  #populate Price Field
                push!(prices,res)
            elseif i == 3 #populate Open Prices
                push!(openPrices,res)
            elseif i == 4 #populate High Prices
                push!(highPrices,res)
            elseif i == 5 #populate Low Prices
                push!(lowPrices,res)
            elseif i == 6 #populate Volume
                push!(volumes,res)
            elseif i == 7
                push!(pChange,res/100)
            end
        end
    end

    df.Date=assetDates
    df.Prices=prices
    df.Open=openPrices
    df.High=highPrices
    df.Low=lowPrices
    df.Vol=volumes
    df.Change=pChange
    
    return df
end

end