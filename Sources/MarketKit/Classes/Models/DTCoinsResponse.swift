//
//  DTCoinsResponse.swift
//

struct DTCoinsResponse: Codable {
    let version: Int
    let coins: [DTCoin]
}

struct DTCoin: Codable {
    let uid: String?
    let image: String?
    let code: String?
    let name: String?
}
