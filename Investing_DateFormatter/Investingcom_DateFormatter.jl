using DataFrames
using CSV
using Dates

function main()

    #Get all csv files in current directory
    flist=filter(endswith(".csv"),readdir())
    
    for i in flist
        df=DataFrame(CSV.File(i))

        df[!,:Date]=Date.(df[!,:Date],"u d, y")

        CSV.write(i,df)
    end
    #println(df)

    
end

main()