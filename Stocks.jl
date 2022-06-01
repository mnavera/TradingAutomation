module Stocks

using DataFrames, WeakRefStrings, OrderedCollections, Statistics

import Dates

export Stock, populate, getStockData, calculateReturns, calculateReturns!, cleanStockData, calculateBeta, betaCoefficient, dropmissing!

include("WebParsers.jl")

import .WebParsers

mutable struct Stock
    name::String
    daily::DataFrame
    weekly::DataFrame
    monthly::DataFrame
    beta::Float64
    position::Int64
end

Stock(s)=Stock(s,DataFrame(),DataFrame(),DataFrame(),0.0,1)
Stock() = Stock("1")


function length(s::Stock,f::Char)
    len::Int64=0
    if f == 'd'
        len=nrow(s.daily)
    elseif f=='w'
        len=nrow(s.weekly)
    elseif f=='m'
        len=nrow(s.monthly)
    else
        error("Not a valid Identifier")
    end
    return len
end

function getStockData(s::Stock,f::Char)
    if f == 'd'
        return s.daily
    elseif f=='w'
        return s.weekly
    elseif f=='m'
        return s.monthly
    else
        error("Not a valid Identifier")
    end
end


function populate(s::Stock,d::DataFrame,w::DataFrame,m::DataFrame,p::Int64)
    s.daily=d
    s.weekly=w
    s.monthly=m
    s.position=p
end

function populate(s::Stock,d::DataFrame,f::Char,p::Int64)
    if f == 'd'
        s.daily=d
    elseif f == 'w'
        s.weekly = d
    elseif f == 'm'
        s.monthly = d
    else
        error("Not a valid identifier")
    end

    s.position=p
end

function populate(s::Stock,d::DataFrame,f::Char)
    if f == 'd'
        s.daily=d
    elseif f == 'w'
        s.weekly = d
    elseif f == 'm'
        s.monthly = d
    else
        error("Not a valid identifier")
    end
end

#Takes 2-column DataFrames only; one for Date, and the other for Asset Prices
function calculateReturns(base_data::DataFrame;ran::Int64=1)
    sort!(base_data)#sort rows by oldest to newest
    temp=similar(base_data)
    if propertynames(temp)[2] != :Returns
        rename!(temp,propertynames(temp)[2]=>:Returns)
    end

    temp[1,:Returns]=0
    temp.Date=base_data.Date
    
    for i in (ran+1):size(base_data,1)
        temp[i,:Returns] = base_data[i,2]/base_data[i-ran,2] - 1
    end

    temp.Returns=checkUndefined(temp.Returns)
    return temp
end

function calculateReturns!(base_data::DataFrame;ran::Int64=1)
    sort!(base_data)#sort rows by oldest to newest
    temp=similar(base_data)
    for i in propertynames(temp)
        if i != :Date
            rename!(temp,i=>:Returns)
            rename!(base_data,i=>:Returns)
        end
    end
    temp[1,:Returns]=0
    temp.Date=base_data.Date
    
    for i in (ran+1):size(base_data,1)
        temp[i,:Returns] = base_data[i,2]/base_data[i-ran,2] - 1
    end
    #temp = calculateReturns(base_data,ran=ran)

    base_data = temp

end

#Takes Stock and 2-column DataFrame arguments
function calculateReturns!(s::Stock,f::Char)
    if f == 'd'
        r=s.daily
    elseif f=='w'
        r=s.weekly
    elseif f=='m'
        r=s.monthly
    else
        error("Not a valid Identifier")
    end

    #Calculate returns and add to dataframe
    ret=calculateReturns(r[:,[:Date,Symbol("Adj Close")]])
    r=leftjoin(r, ret, on=:Date)

    if f == 'd'
        s.daily = r
    elseif f=='w'
        s.weekly = r
    elseif f=='m'
        s.monthly = r
    else
        error("Not a valid Identifier")
    end
end

function calculateReturns(s::Stock,f::Char)
    if f == 'd'
        r=s.daily
    elseif f=='w'
        r=s.weekly
    elseif f=='m'
        r=s.monthly
    else
        error("Not a valid Identifier")
    end

    #Calculate returns and add to dataframe
    ret=calculateReturns(r[:,[:Date,Symbol("Adj Close")]])
    r=leftjoin(r, ret, on=:Date)

    return r
end

#Sanitize data, check for nothing values and replace with missing
function cleanStockData(s::Stock)
    for i in [s.daily,s.weekly,s.monthly]
        for (j,c) in enumerate(eachcol(i))#enumerate over each column
            if any(isnothing,i[:,j])#check if our column has nothing values, replace with missing
                i[!,j] = replace(i[!,j],nothing=> missing)
            end

            if !(typeof(c) == Vector{Dates.Date} || typeof(c) == Vector{Float64} || typeof(c) == Vector{Union{Missing, Float64}})
                i[!,j] = tryparse.(Float64,c)
            end

            if any(isnothing,i[:,j])#check if our column has nothing values, replace with missing
                i[!,j] = replace(i[!,j],nothing=> missing)
            end
            
        end


    end           
end

function dropmissing!(s::Stock,f::Char)
    if f == 'd'
        s.daily=dropmissing(s.daily)
    elseif f=='w'
        s.weekly=dropmissing(s.weekly)
    elseif f=='m'
        s.monthly=dropmissing(s.monthly)
    else
        error("Not a valid Identifier")
    end
end

function calculateBeta(s::Stock,SP500::Stock,dur::Vector{Any}=[])
    betaDict=LittleDict{String,Float64}()
    #Calculate all values of beta we can, with the available data
    d_len=nrow(s.daily)
    w_len=nrow(s.weekly)
    m_len=nrow(s.monthly)

    temp_df_m=sort(s.monthly,rev=true)
    temp_df_w=sort(s.weekly,rev=true)
    temp_df_d=sort(s.daily,rev=true)

    #Calculate all Betas we have that have enough data
    if m_len >= 60
        data=leftjoin(temp_df_m[1:60,[:Date,Symbol("Adj Close")]], SP500.monthly[:,[:Date,Symbol("Adj Close")]], on=:Date, makeunique=true)
        beta=betaCoefficient(data)
        merge!(betaDict,Dict("5Y"=>beta))
    end
    if w_len >= 158
        data=leftjoin(temp_df_w[1:158,[:Date,Symbol("Adj Close")]], SP500.weekly[:,[:Date,Symbol("Adj Close")]], on=:Date, makeunique=true)
        beta=betaCoefficient(data)
        merge!(betaDict,Dict("3Y"=>beta))
    end
    if w_len >= 104
        data=leftjoin(temp_df_w[1:104,[:Date,Symbol("Adj Close")]], SP500.weekly[:,[:Date,Symbol("Adj Close")]], on=:Date, makeunique=true)
        beta=betaCoefficient(data)
        merge!(betaDict,Dict("2Y"=>beta))
    end

    if d_len >= 180
        data=leftjoin(temp_df_d[1:180,[:Date,Symbol("Adj Close")]], SP500.daily[:,[:Date,Symbol("Adj Close")]], on=:Date, makeunique=true)
        beta=betaCoefficient(data)
        merge!(betaDict,Dict("180D"=>beta))
    end

    if d_len >= 90
        data=leftjoin(temp_df_d[1:90,[:Date,Symbol("Adj Close")]], SP500.daily[:,[:Date,Symbol("Adj Close")]], on=:Date, makeunique=true)
        beta=betaCoefficient(data)
        merge!(betaDict,Dict("90D"=>beta))
    end

    
    return betaDict
end


function betaCoefficient(data::DataFrame)
    sp=calculateReturns(data[:,[1,3]])
    asset=calculateReturns(data[:,[1,2]])

    #make sure our x and y values are the same size
    normalized_df=dropmissing(innerjoin(sp,asset,on=:Date,makeunique=true))

    return cov(normalized_df[:,2],normalized_df[:,3])/cov(normalized_df[:,2])
end

#check if we have undefined values and replace with missing
function checkUndefined(arr)
    s=Base.length(arr)
    for i in 1:s
        #print(i,":",isassigned(arr,i))
        if !isassigned(arr,i)
            arr[i]=missing
        end
    end
    return arr
end



end