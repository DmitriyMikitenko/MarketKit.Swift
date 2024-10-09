import Alamofire
import Foundation
import HsToolKit
import ObjectMapper

class HsProvider {
    private let baseUrl: String
    private let networkManager: NetworkManager
    private let appVersion: String
    private let appId: String?
    private let apiKey: String?
    private let dexUrl: String = "https://dev-api.dextrade.com/public/price/by/uuids"

    var proAuthToken: String?

    init(baseUrl: String, networkManager: NetworkManager, appVersion: String, appId: String?, apiKey: String?) {
        self.baseUrl = baseUrl
        self.networkManager = networkManager
        self.appVersion = appVersion
        self.appId = appId
        self.apiKey = apiKey
    }

    private func headers(apiTag: String? = nil, auth: String? = nil) -> HTTPHeaders {
        var headers = HTTPHeaders()
        headers.add(name: "app_platform", value: "ios")
        headers.add(name: "app_version", value: appVersion)

        if let apiTag {
            headers.add(name: "app_tag", value: apiTag)
        }

        if let appId {
            headers.add(name: "app_id", value: appId)
        }

        if let apiKey {
            headers.add(name: "apikey", value: apiKey)
        }

        if let auth {
            headers.add(.authorization(auth))
        }

        return headers
    }

    private func proHeaders(apiTag: String? = nil) -> HTTPHeaders {
        headers(apiTag: apiTag, auth: proAuthToken)
    }
}

extension HsProvider {
    func marketOverview(currencyCode: String) async throws -> MarketOverviewResponse {
        let parameters: Parameters = [
            "simplified": true,
            "currency": currencyCode.lowercased(),
        ]

        return try await networkManager.fetch(url: "\(baseUrl)/v1/markets/overview", method: .get, parameters: parameters, headers: headers())
    }

    func topMoversRaw(currencyCode: String) async throws -> TopMoversRaw {
        let parameters: Parameters = [
            "currency": currencyCode.lowercased(),
        ]

        return try await networkManager.fetch(url: "\(baseUrl)/v1/coins/top-movers", method: .get, parameters: parameters, headers: headers())
    }
}

extension HsProvider {
    // Status

    func status() async throws -> HsStatus {
        try await networkManager.fetch(url: "\(baseUrl)/v1/status/updates", method: .get, headers: headers())
    }

    // Coins

    func allCoins() async throws -> [Coin] {
        try await networkManager.fetch(url: "\(baseUrl)/v1/coins/list", method: .get, headers: headers())
    }

    func allBlockchainRecords() async throws -> [BlockchainRecord] {
        try await networkManager.fetch(url: "\(baseUrl)/v1/blockchains/list", method: .get, headers: headers())
    }

    func allTokenRecords() async throws -> [TokenRecord] {
        try await networkManager.fetch(url: "\(baseUrl)/v1/tokens/list", method: .get, headers: headers())
    }

    // Market Infos

    func marketInfos(top: Int, currencyCode: String, defi: Bool, apiTag: String) async throws -> [MarketInfoRaw] {
        var parameters: Parameters = [
            "limit": top,
            "fields": "price,price_change_24h,market_cap,market_cap_rank,total_volume",
            "currency": currencyCode.lowercased(),
            "order_by_rank": "true",
        ]

        if defi {
            parameters["defi"] = "true"
        }

        return try await networkManager.fetch(url: "\(baseUrl)/v1/coins", method: .get, parameters: parameters, headers: headers(apiTag: apiTag))
    }

    func advancedMarketInfos(top: Int, currencyCode: String) async throws -> [MarketInfoRaw] {
        let parameters: Parameters = [
            "limit": top,
            "currency": currencyCode.lowercased(),
            "order_by_rank": "true",
        ]

        return try await networkManager.fetch(url: "\(baseUrl)/v1/coins/filter", method: .get, parameters: parameters, headers: headers(apiTag: "advanced_search"))
    }

    func marketInfos(coinUids: [String], currencyCode: String, apiTag: String) async throws -> [MarketInfoRaw] {
        let parameters: Parameters = [
            "uids": coinUids.joined(separator: ","),
            "fields": "price,price_change_24h,price_change_7d,price_change_30d,market_cap,market_cap_rank,total_volume",
            "currency": currencyCode.lowercased(),
        ]

        return try await networkManager.fetch(url: "\(baseUrl)/v1/coins", method: .get, parameters: parameters, headers: headers(apiTag: apiTag))
    }

    func marketInfos(categoryUid: String, currencyCode: String, apiTag: String) async throws -> [MarketInfoRaw] {
        let parameters: Parameters = [
            "currency": currencyCode.lowercased(),
        ]

        return try await networkManager.fetch(url: "\(baseUrl)/v1/categories/\(categoryUid)/coins", method: .get, parameters: parameters, headers: headers(apiTag: apiTag))
    }

    func marketInfoOverview(coinUid: String, currencyCode: String, languageCode: String, apiTag: String) async throws -> MarketInfoOverviewResponse {
        let parameters: Parameters = [
            "currency": currencyCode.lowercased(),
            "language": languageCode.lowercased(),
        ]

        return try await networkManager.fetch(url: "\(baseUrl)/v1/coins/\(coinUid)", method: .get, parameters: parameters, headers: headers(apiTag: apiTag))
    }

    func marketInfoTvl(coinUid: String, currencyCode: String, timePeriod: HsTimePeriod) async throws -> [ChartPoint] {
        let parameters: Parameters = [
            "currency": currencyCode.lowercased(),
            "interval": timePeriod.rawValue,
        ]

        let response: [MarketInfoTvlRaw] = try await networkManager.fetch(url: "\(baseUrl)/v1/defi-protocols/\(coinUid)/tvls", method: .get, parameters: parameters, headers: headers())
        return response.compactMap(\.marketInfoTvl)
    }

    func marketInfoGlobalTvl(platform: String, currencyCode: String, timePeriod: HsTimePeriod) async throws -> [ChartPoint] {
        var parameters: Parameters = [
            "currency": currencyCode.lowercased(),
            "interval": timePeriod.rawValue,
        ]

        if !platform.isEmpty {
            parameters["chain"] = platform
        }

        let response: [MarketInfoTvlRaw] = try await networkManager.fetch(url: "\(baseUrl)/v1/global-markets/tvls", method: .get, parameters: parameters, headers: headers())
        return response.compactMap(\.marketInfoTvl)
    }

    func defiCoins(currencyCode: String, apiTag: String) async throws -> [DefiCoinRaw] {
        let parameters: Parameters = [
            "currency": currencyCode.lowercased(),
        ]

        return try await networkManager.fetch(url: "\(baseUrl)/v1/defi-protocols", method: .get, parameters: parameters, headers: headers(apiTag: apiTag))
    }

    // Coin Categories

    func coinCategories(currencyCode: String? = nil) async throws -> [CoinCategory] {
        var parameters: Parameters = [:]
        if let currencyCode {
            parameters["currency"] = currencyCode.lowercased()
        }

        return try await networkManager.fetch(url: "\(baseUrl)/v1/categories", method: .get, parameters: parameters, headers: headers())
    }

    func coinCategoryMarketCapChart(category: String, currencyCode: String?, timePeriod: HsTimePeriod) async throws -> [CategoryMarketPoint] {
        var parameters: Parameters = [:]
        if let currencyCode {
            parameters["currency"] = currencyCode.lowercased()
        }
        parameters["interval"] = timePeriod.rawValue

        return try await networkManager.fetch(url: "\(baseUrl)/v1/categories/\(category)/market_cap", method: .get, parameters: parameters, headers: headers())
    }

    // Coin Prices
    
    func fetchDexPrices(for uids: [String], currencyCode: String) async throws -> [CoinPrice] {
        guard let url = URL(string: dexUrl),
              let body = try? JSONSerialization.data(withJSONObject: uids, options: [])
        else {
            throw NSError(domain: "Invalid Response", code: -1, userInfo: nil)
        }
        var request = URLRequest(url: url)
        request.httpMethod = "POST"
        request.httpBody = body
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")

        // Выполнение GET-запроса
        let (data, response) = try await URLSession.shared.data(from: url)

        // Проверка на статус код ответа
        guard let httpResponse = response as? HTTPURLResponse, (200...299).contains(httpResponse.statusCode) else {
            throw NSError(domain: "Invalid Response", code: -1, userInfo: nil)
        }
        
        // Десериализация ответа
        let jsonObject = try JSONSerialization.jsonObject(with: data, options: [])

        // Проверка на тип объекта (должен быть массив словарей)
        guard let jsonArray = jsonObject as? [[String: Any]] else {
            throw NSError(domain: "Invalid Response", code: -1, userInfo: nil)
        }

        // Преобразование в массив CoinPrice через mapArray
        let coinPrices = try Mapper<CoinPriceResponse>().mapArray(JSONObject: jsonArray)

        // Возвращаем массив объектов CoinPrice
        return coinPrices.map { $0.coinPrice(currencyCode: currencyCode) }
    }
    
    func coinPrices(coinUids: [String], walletCoinUids: [String], currencyCode: String) async throws -> [CoinPrice] {
        // Создаем массив для хранения всех цен
        var resultCoinPrices: [CoinPrice] = []

        // Фильтруем coinUids
        var hsProviderUids = coinUids.filter { $0 != "philtoken" }
        
        // Если "philtoken" в списке, выполняем fetchDexPrices
        if hsProviderUids.count != coinUids.count {
            let dexPrices = try await fetchDexPrices(for: ["philtoken"], currencyCode: currencyCode)
            resultCoinPrices.append(contentsOf: dexPrices)  // Добавляем результат fetchDexPrices
        }

        // Подготавливаем параметры для второго запроса
        var parameters: Parameters = [
            "uids": hsProviderUids.joined(separator: ","),
            "currency": currencyCode.lowercased(),
            "fields": "price,price_change_24h,last_updated"
        ]

        // Если есть walletCoinUids, добавляем их в параметры
        if !walletCoinUids.isEmpty {
            parameters["enabled_uids"] = walletCoinUids.joined(separator: ",")
        }

        // Выполняем второй запрос
        let responses: [CoinPriceResponse] = try await networkManager.fetch(url: "\(baseUrl)/v1/coins", method: .get, parameters: parameters, headers: headers(apiTag: "coin_prices"))
        
        // Преобразуем responses в CoinPrice и добавляем к результатам
        resultCoinPrices.append(contentsOf: responses.map { $0.coinPrice(currencyCode: currencyCode) })
        
        // Возвращаем результирующий массив
        return resultCoinPrices
    }

//    func coinPrices(coinUids: [String], walletCoinUids: [String], currencyCode: String) async throws -> [CoinPrice] {
//        var hsProviderUids = coinUids.filter { $0 != "philtoken" }
//        if hsProviderUids.count != coinUids.count {
//            let coinPrice = try await fetchDexPrices(for: ["philtoken"], currencyCode: currencyCode)
//            print(coinPrice)
//        }
//        
//        var parameters: Parameters = [
//            "uids": hsProviderUids.joined(separator: ","),
//            "currency": currencyCode.lowercased(),
//            "fields": "price,price_change_24h,last_updated",
//        ]
//
//        if !walletCoinUids.isEmpty {
//            parameters["enabled_uids"] = walletCoinUids.joined(separator: ",")
//        }
//
//        let responses: [CoinPriceResponse] = try await networkManager.fetch(url: "\(baseUrl)/v1/coins", method: .get, parameters: parameters, headers: headers(apiTag: "coin_prices"))
//        return responses.map { $0.coinPrice(currencyCode: currencyCode) }
//    }

    func historicalCoinPrice(coinUid: String, currencyCode: String, timestamp: TimeInterval) async throws -> HistoricalCoinPriceResponse {
        let parameters: Parameters = [
            "currency": currencyCode.lowercased(),
            "timestamp": Int(timestamp),
        ]

        return try await networkManager.fetch(url: "\(baseUrl)/v1/coins/\(coinUid)/price_history", method: .get, parameters: parameters, headers: headers())
    }

    func coinPriceChartStart(coinUid: String) async throws -> ChartStart {
        try await networkManager.fetch(url: "\(baseUrl)/v1/coins/\(coinUid)/price_chart_start", method: .get, headers: headers())
    }

    func topPlatformMarketCapStart(platform: String) async throws -> ChartStart {
        try await networkManager.fetch(url: "\(baseUrl)/v1/top-platforms/\(platform)/market_chart_start", method: .get, headers: headers())
    }

    func coinPriceChart(coinUid: String, currencyCode: String, interval: HsPointTimePeriod, fromTimestamp: TimeInterval? = nil) async throws -> [ChartCoinPriceResponse] {
        var parameters: Parameters = [
            "currency": currencyCode.lowercased(),
            "interval": interval.rawValue,
        ]

        if let fromTimestamp {
            parameters["from_timestamp"] = Int(fromTimestamp)
        }

        return try await networkManager.fetch(url: "\(baseUrl)/v1/coins/\(coinUid)/price_chart", method: .get, parameters: parameters, headers: headers())
    }

    // Holders

    func tokenHolders(coinUid: String, blockchainUid: String) async throws -> TokenHolders {
        let parameters: Parameters = [
            "blockchain_uid": blockchainUid,
        ]

        return try await networkManager.fetch(url: "\(baseUrl)/v1/analytics/\(coinUid)/holders", method: .get, parameters: parameters, headers: proHeaders())
    }

    // Funds

    func coinInvestments(coinUid: String) async throws -> [CoinInvestment] {
        let parameters: Parameters = [
            "coin_uid": coinUid,
        ]

        return try await networkManager.fetch(url: "\(baseUrl)/v1/funds/investments", method: .get, parameters: parameters, headers: headers())
    }

    func coinTreasuries(coinUid: String, currencyCode: String) async throws -> [CoinTreasury] {
        let parameters: Parameters = [
            "coin_uid": coinUid,
            "currency": currencyCode.lowercased(),
        ]

        return try await networkManager.fetch(url: "\(baseUrl)/v1/funds/treasuries", method: .get, parameters: parameters, headers: headers())
    }

    func coinReports(coinUid: String) async throws -> [CoinReport] {
        let parameters: Parameters = [
            "coin_uid": coinUid,
        ]

        return try await networkManager.fetch(url: "\(baseUrl)/v1/reports", method: .get, parameters: parameters, headers: headers())
    }

    func twitterUsername(coinUid: String) async throws -> String? {
        let response: TwitterUsernameResponse = try await networkManager.fetch(url: "\(baseUrl)/v1/coins/\(coinUid)/twitter", method: .get, headers: headers())
        return response.username
    }

    func globalMarketPoints(currencyCode: String, timePeriod: HsTimePeriod) async throws -> [GlobalMarketPoint] {
        let parameters: Parameters = [
            "interval": timePeriod.rawValue,
            "currency": currencyCode,
        ]

        return try await networkManager.fetch(url: "\(baseUrl)/v1/global-markets", method: .get, parameters: parameters, headers: headers())
    }

    // Top Pairs

    func topPairs(currencyCode: String) async throws -> [MarketPair] {
        let parameters: Parameters = [
            "currency": currencyCode.lowercased(),
        ]

        return try await networkManager.fetch(url: "\(baseUrl)/v1/exchanges/top-pairs", method: .get, parameters: parameters, headers: headers())
    }

    // Top Platforms

    func topPlatforms(currencyCode: String) async throws -> [TopPlatformResponse] {
        let parameters: Parameters = [
            "currency": currencyCode.lowercased(),
        ]

        return try await networkManager.fetch(url: "\(baseUrl)/v1/top-platforms", method: .get, parameters: parameters, headers: headers())
    }

    func topPlatformCoinsList(blockchain: String, currencyCode: String, apiTag: String) async throws -> [MarketInfoRaw] {
        let parameters: Parameters = [
            "currency": currencyCode.lowercased(),
        ]

        return try await networkManager.fetch(url: "\(baseUrl)/v1/top-platforms/\(blockchain)/list", method: .get, parameters: parameters, headers: headers(apiTag: apiTag))
    }

    func topPlatformMarketCapChart(platform: String, currencyCode: String?, interval: HsPointTimePeriod, fromTimestamp: TimeInterval? = nil) async throws -> [CategoryMarketPoint] {
        var parameters: Parameters = [
            "interval": interval.rawValue,
        ]
        if let currencyCode {
            parameters["currency"] = currencyCode.lowercased()
        }
        if let fromTimestamp {
            parameters["from_timestamp"] = Int(fromTimestamp)
        }

        return try await networkManager.fetch(url: "\(baseUrl)/v1/top-platforms/\(platform)/market_chart", method: .get, parameters: parameters, headers: headers())
    }

    // Pro Charts

    private func proData<T: ImmutableMappable>(path: String, currencyCode: String, timePeriod: HsTimePeriod) async throws -> [T] {
        let parameters: Parameters = [
            "currency": currencyCode.lowercased(),
            "interval": timePeriod.rawValue,
        ]

        return try await networkManager.fetch(url: "\(baseUrl)/v1/analytics/\(path)", method: .get, parameters: parameters, headers: proHeaders())
    }

    private func proData<T: ImmutableMappable>(path: String, timePeriod: HsTimePeriod) async throws -> [T] {
        let parameters: Parameters = [
            "interval": timePeriod.rawValue,
        ]

        return try await networkManager.fetch(url: "\(baseUrl)/v1/analytics/\(path)", method: .get, parameters: parameters, headers: proHeaders())
    }

    private func rankData<T: ImmutableMappable>(type: String, currencyCode: String? = nil) async throws -> [T] {
        var parameters: Parameters = [
            "type": type,
        ]

        if let currencyCode {
            parameters["currency"] = currencyCode.lowercased()
        }

        return try await networkManager.fetch(url: "\(baseUrl)/v1/analytics/ranks", method: .get, parameters: parameters, headers: proHeaders())
    }

    func analytics(coinUid: String, currencyCode: String, apiTag: String) async throws -> Analytics {
        let parameters: Parameters = [
            "currency": currencyCode.lowercased(),
        ]
        return try await networkManager.fetch(url: "\(baseUrl)/v1/analytics/\(coinUid)", method: .get, parameters: parameters, headers: proHeaders(apiTag: apiTag))
    }

    func analyticsPreview(coinUid: String, apiTag: String) async throws -> AnalyticsPreview {
        try await networkManager.fetch(url: "\(baseUrl)/v1/analytics/\(coinUid)/preview", method: .get, headers: headers(apiTag: apiTag))
    }

    func dexVolumes(coinUid: String, currencyCode: String, timePeriod: HsTimePeriod) async throws -> [VolumePoint] {
        try await proData(path: "\(coinUid)/dex-volumes", currencyCode: currencyCode, timePeriod: timePeriod)
    }

    func dexLiquidity(coinUid: String, currencyCode: String, timePeriod: HsTimePeriod) async throws -> [VolumePoint] {
        try await proData(path: "\(coinUid)/dex-liquidity", currencyCode: currencyCode, timePeriod: timePeriod)
    }

    func activeAddresses(coinUid: String, timePeriod: HsTimePeriod) async throws -> [CountPoint] {
        try await proData(path: "\(coinUid)/addresses", timePeriod: timePeriod)
    }

    func transactions(coinUid: String, timePeriod: HsTimePeriod) async throws -> [CountVolumePoint] {
        try await proData(path: "\(coinUid)/transactions", timePeriod: timePeriod)
    }

    func cexVolumeRanks(currencyCode: String) async throws -> [RankMultiValue] {
        try await rankData(type: "cex_volume", currencyCode: currencyCode)
    }

    func dexVolumeRanks(currencyCode: String) async throws -> [RankMultiValue] {
        try await rankData(type: "dex_volume", currencyCode: currencyCode)
    }

    func dexLiquidityRanks() async throws -> [RankValue] {
        try await rankData(type: "dex_liquidity")
    }

    func activeAddressRanks() async throws -> [RankMultiValue] {
        try await rankData(type: "address")
    }

    func transactionCountRanks() async throws -> [RankMultiValue] {
        try await rankData(type: "tx_count")
    }

    func holdersRanks() async throws -> [RankValue] {
        try await rankData(type: "holders")
    }

    func feeRanks(currencyCode: String) async throws -> [RankMultiValue] {
        try await rankData(type: "fee", currencyCode: currencyCode)
    }

    func revenueRanks(currencyCode: String) async throws -> [RankMultiValue] {
        try await rankData(type: "revenue", currencyCode: currencyCode)
    }

    // Authentication

    func subscriptions(addresses: [String]) async throws -> [ProSubscription] {
        let parameters: Parameters = [
            "address": addresses.joined(separator: ","),
        ]

        return try await networkManager.fetch(url: "\(baseUrl)/v1/analytics/subscriptions", method: .get, parameters: parameters, headers: headers())
    }

    func authKey(address: String) async throws -> String {
        let parameters: Parameters = [
            "address": address,
        ]

        let response: SignMessageResponse = try await networkManager.fetch(url: "\(baseUrl)/v1/auth/get-sign-message", method: .get, parameters: parameters, headers: headers())

        return response.message
    }

    func authenticate(signature: String, address: String) async throws -> String {
        let parameters: Parameters = [
            "signature": signature,
            "address": address,
        ]

        let response: AuthenticateResponse = try await networkManager.fetch(url: "\(baseUrl)/v1/auth/authenticate", method: .post, parameters: parameters, headers: headers())

        return response.token
    }

    // Personal Support

    func requestPersonalSupport(telegramUsername: String) async throws {
        let parameters: Parameters = [
            "username": telegramUsername,
        ]

        _ = try await networkManager.fetchJson(url: "\(baseUrl)/v1/support/start-chat", method: .post, parameters: parameters, headers: proHeaders())
    }

    // Market Tickers

    func marketTickers(coinUid: String) async throws -> [MarketTicker] {
        try await networkManager.fetch(url: "\(baseUrl)/v1/exchanges/tickers/\(coinUid)", method: .get, headers: headers())
    }
}

extension HsProvider {
    struct HistoricalCoinPriceResponse: ImmutableMappable {
        let timestamp: Int
        let price: Decimal

        init(map: Map) throws {
            timestamp = try map.value("timestamp")
            price = try map.value("price", using: Transform.stringToDecimalTransform)
        }
    }

    struct ChartCoinPriceResponse: ImmutableMappable {
        let timestamp: Int
        let price: Decimal
        let totalVolume: Decimal?

        init(map: Map) throws {
            timestamp = try map.value("timestamp")
            price = try map.value("price", using: Transform.stringToDecimalTransform)
            totalVolume = try? map.value("volume", using: Transform.stringToDecimalTransform)
        }

        var chartPoint: ChartPoint {
            ChartPoint(
                timestamp: TimeInterval(timestamp),
                value: price,
                volume: totalVolume
            )
        }

        var volumeChartPoint: ChartPoint? {
            guard let totalVolume else {
                return nil
            }

            return ChartPoint(timestamp: TimeInterval(timestamp), value: totalVolume)
        }
    }

    struct SignMessageResponse: ImmutableMappable {
        let message: String

        init(map: Map) throws {
            message = try map.value("message")
        }
    }

    struct AuthenticateResponse: ImmutableMappable {
        let token: String

        init(map: Map) throws {
            token = try map.value("token")
        }
    }
}
