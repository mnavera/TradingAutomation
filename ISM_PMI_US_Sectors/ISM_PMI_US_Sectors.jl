using DataFrames
using CSV
using Dates
using XLSX

include("../ENV.jl")

mutable struct Ranks
    name::String
    data::DataFrame
end 

Ranks(s)=Ranks(s,DataFrame())
Ranks() = Ranks("1")

function parseToDataFrame(data,scores,zeroChange)
    growth_ind=[]
    fullList=vec(hcat(data...,zeroChange...))

    for i in scores
        if (i > 0)
            push!(growth_ind,"Growth")
        elseif (i == 0)
            push!(growth_ind,"Neutral")
        else
            push!(growth_ind,"Contraction")
        end
    end
    df=DataFrame(:Industry=>fullList,:Growth=>growth_ind,:Score=>scores)

    return df
end


function getScoreArray(upperBound,spliceIndex)#upperBound=max score to give, spliceIndex=how many zeros to append at end of array
    lowerBound=upperBound-18
    scoreArray=reverse(filter(x->x≠0, collect(lowerBound:upperBound)))#get the array of scores, remove zeroes, then order by largest to smallest

    zeroVals=zeros(Int8,18-spliceIndex)#count number of industries that are neither growing nor slowing
    
    splice!(scoreArray, (spliceIndex+1):length(scoreArray),zeroVals)#splice end of array and replace with zeros

    return scoreArray
    
end


# ---TESTING GROUND---
function rankIndustries(data, data_ref=REF, growth_num=18,flip=1)
    unranked=Dict{Any,Any}() #prepare ordered list of industries
    noChange=[]#collect the industries that dont appear=no change
    #put industries in the order they appear in a Dict
    for i in data_ref
        result = findfirst(i,data)
        if isnothing(result)
            #println(i,": 0") #for industries with no change
            push!(noChange,i)
        else
            unranked[first(result)]=i
        end
    end
    sArray=[]
    upperBound=length(unranked)

    sArray=getScoreArray(growth_num,upperBound)

    map!(x -> x * flip, sArray, sArray[1:upperBound])
    if flip == -1
        reverse!(sArray,1,upperBound)
    end

    ranked=values(sort(unranked))#get an array instead of a dict since we don't need the keys anymore
    
    prev_rank=[]
    curr_rank=[]

    curr_df=parseToDataFrame(ranked,sArray,noChange)

    current_list=curr_df[:,:Industry]


    #find previous ranking of items in current data
    for v in current_list
        push!(prev_rank,findfirst(x->x==v,data_ref))
    end


    curr_df.Previous_Rank=prev_rank
    sort!(curr_df, [:Previous_Rank])
    return curr_df
end



function main()
    #Location for upload
    fpath=FILEPATH*"/ISM_PMI_US_Sectors/"
    
    #Delete old CSVs in directory before adding in new ones
    #rm(FILEPATH, recursive=true, force=true)
    #mkpath(FILEPATH)

    #list of all industries
    REF=[
        "Apparel, Leather & Allied Products",
        "Furniture & Related Products",
        "Wood Products",
        "Fabricated Metal Products",
        "Machinery",
        "Computer & Electronic Products",
        "Transportation Equipment",
        "Plastics & Rubber Products",
        "Paper Products",
        "Chemical Products",
        "Petroleum & Coal Products",
        "Primary Metals",
        "Textile Mills",
        "Electrical Equipment, Appliances & Components",
        "Food, Beverage & Tobacco Products",
        "Miscellaneous Manufacturing",
        "Nonmetallic Mineral Products", 
        "Printing & Related Support Activities"
    ]

    #name of the Indexes that matter
    #SECTOR_INDEX=["MAN_PMI","MAN_NEWORDERS","MAN_PROD","MAN_EMPL","MAN_DELIV","MAN_INVENT","MAN_CUSTINV"]



    #Rows 1-18 are the previous ranking 
    #row 19 is number reported growing/slowing depending on usage 
    #row 20 is the actual paragraph, 
    #row 21 is a bit flip that determines whether treat the list as the list of growing or the list of slowing
    initial_data_ref = CSV.read(pwd()*"/ISM_PMI_US_Sectors/ISM_PMI_US_Sectors.csv", DataFrame)

    #Get copy of DataFrame to preserve data
    initial_data = copy(initial_data_ref)


    #The string to parse
    #data_reference,grow,blob,bflip=initial_data.MAN_DELIV[1:18],parse(Int64,initial_data.MAN_DELIV[19]),initial_data.MAN_DELIV[20],parse(Int64,initial_data.MAN_DELIV[21])


    #Check for the order in which the industries appear
    
    #df_ranked=rankIndustries(blob,data_reference,grow,bflip)
    #println(df_ranked)

    DATA_BLOB=[]

    for n in (propertynames(initial_data))
        println("[+] Processing Data for ",n,"...")
        rank=Ranks(String(n))
        data_reference,grow,blob,bflip=initial_data[1:18, n],parse(Int64,initial_data[19, n]),initial_data[20,n],parse(Int64,initial_data[21,n])
        try
            #Check for the order in which the industries appear
            rank.data=rankIndustries(blob,data_reference,grow,bflip)
            push!(DATA_BLOB,rank)
        catch e
            println("ERROR")
        end



    end
    filename=fpath*"ISM_PMI_Sectors.xlsx"
    XLSX.openxlsx(filename,mode="w") do xf
        for i in DATA_BLOB
            XLSX.addsheet!(xf,i.name)
            XLSX.writetable!(xf[i.name],i.data)
        end
    end
end

main()
