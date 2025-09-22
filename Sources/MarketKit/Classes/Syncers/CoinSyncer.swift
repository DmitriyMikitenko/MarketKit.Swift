import Combine
import Foundation
import HsExtensions

class CoinSyncer {
    private let keyCoinsLastSyncTimestamp = "coin-syncer-coins-last-sync-timestamp"
    private let keyBlockchainsLastSyncTimestamp = "coin-syncer-blockchains-last-sync-timestamp"
    private let keyTokensLastSyncTimestamp = "coin-syncer-tokens-last-sync-timestamp"
    private let keyInitialSyncVersion = "coin-syncer-initial-sync-version"
    
    private let keyDextradeCoinsLastSyncTimestamp = "coin-syncer-dextrade-coins-last-sync-timestamp"
    private let keyDextradeTokensLastSyncTimestamp = "coin-syncer-dextrade-tokens-last-sync-timestamp"
    
    private let limit = 1000
    private let currentVersion = 15

    private let storage: CoinStorage
    private let hsProvider: HsProvider
    private let syncerStateStorage: SyncerStateStorage
    private var tasks = Set<AnyTask>()

    private let fullCoinsUpdatedSubject = PassthroughSubject<Void, Never>()

    init(storage: CoinStorage, hsProvider: HsProvider, syncerStateStorage: SyncerStateStorage) {
        self.storage = storage
        self.hsProvider = hsProvider
        self.syncerStateStorage = syncerStateStorage
    }

    private func saveLastSyncTimestamps(coins: Int, blockchains: Int, tokens: Int) {
        try? syncerStateStorage.save(value: String(coins), key: keyCoinsLastSyncTimestamp)
        try? syncerStateStorage.save(value: String(blockchains), key: keyBlockchainsLastSyncTimestamp)
        try? syncerStateStorage.save(value: String(tokens), key: keyTokensLastSyncTimestamp)
    }
    
    private func saveLastDTSyncTimestamps(coins: Int, tokens: Int) {
        try? syncerStateStorage.save(value: String(coins), key: keyDextradeCoinsLastSyncTimestamp)
        try? syncerStateStorage.save(value: String(tokens), key: keyDextradeTokensLastSyncTimestamp)
    }
    
    func newCoins() -> [Coin] {
        var newCoins = [Coin]()
        let xdcCoin = Coin(uid: "xdce-crowd-sale", name: "XDC Network", code: "XDC")
        let mewCoin = Coin(uid: "cat-in-a-dogs-world;", name: "cat in a dogs world", code: "MEW", marketCapRank: 150, coinGeckoId: "cat-in-a-dogs-world;")
        let philCoin = Coin(uid: "philtoken", name: "Philtoken", code: "PHIL", marketCapRank: 1, coinGeckoId: "philtoken")
                
        newCoins.append(contentsOf: [xdcCoin, mewCoin, philCoin])
        
        return newCoins
    }
    
    func newBlockchains() -> [BlockchainRecord] {
        var blockchainRecords = [BlockchainRecord]()
        let xdcBlockchain = BlockchainRecord(uid: "xdc-network", name: "xdc-network")
        let lightningBlockchain = BlockchainRecord(uid: "lightning-bitcoin", name: "Lightning Bitcoin")
        blockchainRecords.append(contentsOf: [xdcBlockchain, lightningBlockchain])
        
        return blockchainRecords
    }
    
    func newTokens() -> [TokenRecord] {
        var tokenRecords = [TokenRecord]()
        let xdcToken = TokenRecord(coinUid: "xdce-crowd-sale", blockchainUid: "xdc-network", type: "native", decimals: 18)
        let mewToken = TokenRecord(coinUid: "cat-in-a-dogs-world", blockchainUid: "solana", type: "spl", decimals: 5, reference: "MEW1gQWJ3nEXg2qgERiKu7FAFj79PHvQVREQUzScPP5")
        let philToken = TokenRecord(coinUid: "philtoken", blockchainUid: "ethereum", type: "eip20", decimals: 18, reference: "0xc328a59E7321747aEBBc49FD28d1b32C1af8d3b2")
        let elrondToken = TokenRecord(coinUid: "elrond-erd-2", blockchainUid: "elrond-erd-2", type: "native", decimals: 18)
        let btcLightningToken = TokenRecord(coinUid: "bitcoin", blockchainUid: "lightning-bitcoin", type: "native", decimals: 8)
        let usdcMaticToken = TokenRecord(coinUid: "usd-coin", blockchainUid: "polygon-pos", type: "eip20", decimals: 6, reference: "0x3c499c542cef5e3811e1192ce70d8cc03d5c3359")
        
        tokenRecords.append(contentsOf: [xdcToken, mewToken, philToken, elrondToken, btcLightningToken, usdcMaticToken])
        
        return tokenRecords
    }

    private func handleFetched(coins: [Coin], blockchainRecords: [BlockchainRecord], tokenRecords: [TokenRecord]) {
        let newCoins = coins + newCoins()
        let newBlockchains = blockchainRecords + newBlockchains()
        let newTokens = tokenRecords + newTokens()
        
        do {
            try storage.update(coins: newCoins, blockchainRecords: newBlockchains, tokenRecords: transform(tokenRecords: newTokens))
            fullCoinsUpdatedSubject.send()
        } catch {
            print("Fetched data error: \(error)")
        }
    }

    private func transform(tokenRecords: [TokenRecord], blockchainUid: String, types: [String]) -> [TokenRecord] {
        var tokenRecords = tokenRecords

        if let index = tokenRecords.firstIndex(where: { $0.blockchainUid == blockchainUid && $0.type == "native" }) {
            let record = tokenRecords[index]
            tokenRecords.remove(at: index)

            tokenRecords.append(contentsOf:
                types.map {
                    TokenRecord(
                        coinUid: record.coinUid,
                        blockchainUid: record.blockchainUid,
                        type: $0,
                        decimals: record.decimals
                    )
                }
            )
        }

        return tokenRecords
    }

    private func transform(tokenRecords: [TokenRecord]) -> [TokenRecord] {
        let derivationTypes = TokenType.Derivation.allCases.map { "derived:\($0.rawValue)" }
        let addressTypes = TokenType.AddressType.allCases.map { "address_type:\($0.rawValue)" }

        var tokenRecords = transform(tokenRecords: tokenRecords, blockchainUid: BlockchainType.bitcoin.uid, types: derivationTypes)
        tokenRecords = transform(tokenRecords: tokenRecords, blockchainUid: BlockchainType.litecoin.uid, types: derivationTypes)
        return transform(tokenRecords: tokenRecords, blockchainUid: BlockchainType.bitcoinCash.uid, types: addressTypes)
    }
}

extension CoinSyncer {
    var fullCoinsUpdatedPublisher: AnyPublisher<Void, Never> {
        fullCoinsUpdatedSubject.eraseToAnyPublisher()
    }

    func initialSync() {
        do {
            if let versionString = try syncerStateStorage.value(key: keyInitialSyncVersion), let version = Int(versionString), currentVersion == version {
                return
            }

            guard let coinsPath = Bundle.module.url(forResource: "coins", withExtension: "json", subdirectory: "Dumps") else {
                return
            }
            guard let blockchainsPath = Bundle.module.url(forResource: "blockchains", withExtension: "json", subdirectory: "Dumps") else {
                return
            }
            guard let tokensPath = Bundle.module.url(forResource: "tokens", withExtension: "json", subdirectory: "Dumps") else {
                return
            }

            guard let coins = try [Coin](JSONString: String(contentsOf: coinsPath, encoding: .utf8)) else {
                return
            }
            guard let blockchainRecords = try [BlockchainRecord](JSONString: String(contentsOf: blockchainsPath, encoding: .utf8)) else {
                return
            }
            guard let tokenRecords = try [TokenRecord](JSONString: String(contentsOf: tokensPath, encoding: .utf8)) else {
                return
            }
            
            let allCoins = coins + newCoins()
            let allBlockchains = blockchainRecords + newBlockchains()
            let allTokens = tokenRecords + newTokens()

            try storage.update(coins: allCoins, blockchainRecords: allBlockchains, tokenRecords: transform(tokenRecords: allTokens))

            try syncerStateStorage.save(value: "\(currentVersion)", key: keyInitialSyncVersion)
            try syncerStateStorage.delete(key: keyCoinsLastSyncTimestamp)
            try syncerStateStorage.delete(key: keyBlockchainsLastSyncTimestamp)
            try syncerStateStorage.delete(key: keyTokensLastSyncTimestamp)
        } catch {
            print("CoinSyncer: initial sync error: \(error)")
        }
    }

    func coinsDump() throws -> String? {
        let coins = try storage.allCoins()
        return coins.toJSONString()
    }

    func blockchainsDump() throws -> String? {
        let blockchainRecords = try storage.allBlockchainRecords()
        return blockchainRecords.toJSONString()
    }

    func tokenRecordsDump() throws -> String? {
        let tokenRecords = try storage.allTokenRecords()
        return tokenRecords.toJSONString()
    }
    
    func syncRemote() {
        Task {
            do {
                var coinsOutdated = true
                var tokensOutdated = true
                
                let remoteCoinsResponse = try await DTProvider.remoteCoins()
                let remoteTokensResponse = try await DTProvider.remoteTokens()
                
                let remoteCoinsLastTimestamp = remoteCoinsResponse.version
                let remoteTokensLastTimestamp = remoteTokensResponse.version
                
                if let rawLastSyncTimestamp = try? syncerStateStorage.value(key: keyDextradeCoinsLastSyncTimestamp),
                   let localLastSyncTimestamp = Int(rawLastSyncTimestamp),
                   remoteCoinsLastTimestamp <= localLastSyncTimestamp {
                    coinsOutdated = false
                }
                
                if let rawLastSyncTimestamp = try? syncerStateStorage.value(key: keyDextradeTokensLastSyncTimestamp),
                   let localLastSyncTimestamp = Int(rawLastSyncTimestamp),
                   remoteTokensLastTimestamp <= localLastSyncTimestamp {
                    tokensOutdated = false
                }
                
                guard coinsOutdated || tokensOutdated else {
                    return
                }
                
                let remoteCoins: [Coin] = remoteCoinsResponse.coins.compactMap { convertToCoinModel(dextradeCoin: $0) }
                let remoteTokens: [TokenRecord] = remoteTokensResponse.tokens.compactMap { convertToTokenRecord(dextradeToken: $0) }
                
                let currentCoins = try storage.allCoins()
                let currentTokens = try storage.allTokenRecords()
                let currentBlockchains = try storage.allBlockchainRecords()
                
                let currentCoinsSet = Set(currentCoins)
                let mergedCoins = currentCoins + remoteCoins.filter { !currentCoinsSet.contains($0) }
                                
                let currentTokensSet = Set(currentTokens)
                let mergedTokens = currentTokens + remoteTokens.filter { !currentTokensSet.contains($0) }
                
                try storage.update(coins: mergedCoins, blockchainRecords: currentBlockchains, tokenRecords: mergedTokens)
                saveLastDTSyncTimestamps(coins: remoteCoinsLastTimestamp, tokens: remoteTokensLastTimestamp)
            } catch {
                print("Error while trying fetch or handle data from Dextrade: \(error)")
            }
        }.store(in: &tasks)
    }
    
    private func convertToCoinModel(dextradeCoin: DTCoin) -> Coin? {
        guard let uid = dextradeCoin.uid,
              let name = dextradeCoin.name,
              let code = dextradeCoin.code,
              !uid.isEmpty,
              !name.isEmpty,
              !code.isEmpty else {
            return nil
        }
        
        return Coin(
            uid: uid,
            name: name,
            code: code
        )
    }
    
    private func convertToTokenRecord(dextradeToken: DTToken) -> TokenRecord? {
        guard let coinUid = dextradeToken.coin_uid,
              let blockchain = correctBlockchainUid(old: dextradeToken.blockchain_uid),
              let type = dextradeToken.type,
              let decimals = dextradeToken.decimals,
              !coinUid.isEmpty,
              !blockchain.isEmpty,
              !type.isEmpty else {
            return nil
        }
        
        return TokenRecord(
            coinUid: coinUid,
            blockchainUid: blockchain,
            type: type,
            decimals: decimals,
            reference: dextradeToken.address?.lowercased()
        )
    }
    
    private func correctBlockchainUid(old: String?) -> String? {
        guard let old else {
            return nil
        }
        
        switch old {
        case "polygon":
            return "polygon-pos"
        case "arbitrumOne":
            return "arbitrum-one"
        case "optimism":
            return "optimistic-ethereum"
        case "binance_smart_chain":
            return "binance-smart-chain"
        case "binance-chain":
            return nil
        case "":
            return nil
        default:
            return old
        }
    }

    func sync(coinsTimestamp: Int, blockchainsTimestamp: Int, tokensTimestamp: Int) {
        var coinsOutdated = true
        var blockchainsOutdated = true
        var tokensOutdated = true

        if let rawLastSyncTimestamp = try? syncerStateStorage.value(key: keyCoinsLastSyncTimestamp), let lastSyncTimestamp = Int(rawLastSyncTimestamp), coinsTimestamp == lastSyncTimestamp {
            coinsOutdated = false
        }
        if let rawLastSyncTimestamp = try? syncerStateStorage.value(key: keyBlockchainsLastSyncTimestamp), let lastSyncTimestamp = Int(rawLastSyncTimestamp), blockchainsTimestamp == lastSyncTimestamp {
            blockchainsOutdated = false
        }
        if let rawLastSyncTimestamp = try? syncerStateStorage.value(key: keyTokensLastSyncTimestamp), let lastSyncTimestamp = Int(rawLastSyncTimestamp), tokensTimestamp == lastSyncTimestamp {
            tokensOutdated = false
        }

        guard coinsOutdated || blockchainsOutdated || tokensOutdated else {
            return
        }

        Task { [weak self, hsProvider] in
            do {
                async let coins = try hsProvider.allCoins()
                async let blockchainRecords = try hsProvider.allBlockchainRecords()
                async let tokenRecords = try hsProvider.allTokenRecords()

                try await self?.handleFetched(coins: coins, blockchainRecords: blockchainRecords, tokenRecords: tokenRecords)
                self?.saveLastSyncTimestamps(coins: coinsTimestamp, blockchains: blockchainsTimestamp, tokens: tokensTimestamp)
            } catch {
                print("Market data fetch error: \(error)")
            }
        }.store(in: &tasks)
    }

    func syncInfo() -> Kit.SyncInfo {
        Kit.SyncInfo(
            coinsTimestamp: try? syncerStateStorage.value(key: keyCoinsLastSyncTimestamp),
            blockchainsTimestamp: try? syncerStateStorage.value(key: keyBlockchainsLastSyncTimestamp),
            tokensTimestamp: try? syncerStateStorage.value(key: keyTokensLastSyncTimestamp)
        )
    }
}
