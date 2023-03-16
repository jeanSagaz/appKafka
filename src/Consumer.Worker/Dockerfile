#See https://aka.ms/containerfastmode to understand how Visual Studio uses this Dockerfile to build your images for faster debugging.

FROM mcr.microsoft.com/dotnet/runtime:6.0 AS base
WORKDIR /app

FROM mcr.microsoft.com/dotnet/sdk:6.0 AS build
WORKDIR /src
COPY ["src/Consumer.Worker/Consumer.Worker.csproj", "src/Consumer.Worker/"]
RUN dotnet restore "src/Consumer.Worker/Consumer.Worker.csproj"
COPY . .
WORKDIR "/src/src/Consumer.Worker"
RUN dotnet build "Consumer.Worker.csproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "Consumer.Worker.csproj" -c Release -o /app/publish

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "Consumer.Worker.dll"]