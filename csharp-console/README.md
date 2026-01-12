# 部屬與開發

``` cmd
# 開發
cd csharp-console
dotnet run

# 部屬
dotnet publish -c Release -r win-x64 --self-contained true -p:PublishSingleFile=true
```

