//
//  DTTokensResponse.swift
//

struct DTTokensResponse: Codable {
    let version: Int
    let tokens: [DTToken]
}

struct DTToken: Codable {
    let blockchain_uid: String?
    let coin_uid: String?
    let type: String?
    let address: String?
    let decimals: Int?
}

//{"address":"0xD82544bf0dfe8385eF8FA34D67e6e4940CC63e16","decimals":6,"blockchain_uid":"binance_smart_chain","type":"eip20","coin_uid":"myx-finance"
