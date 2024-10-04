import Combine
import Foundation
import HsExtensions

class CoinSyncer {
    private let keyCoinsLastSyncTimestamp = "coin-syncer-coins-last-sync-timestamp"
    private let keyBlockchainsLastSyncTimestamp = "coin-syncer-blockchains-last-sync-timestamp"
    private let keyTokensLastSyncTimestamp = "coin-syncer-tokens-last-sync-timestamp"
    private let keyInitialSyncVersion = "coin-syncer-initial-sync-version"
    private let limit = 1000
    private let currentVersion = 3

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
    
    func newCoins() -> [Coin] {
        var newCoins = [Coin]()
        let xdcCoin = Coin(uid: "xdce-crowd-sale", name: "XDC Network", code: "XDC")
        let mewCoin = Coin(uid: "cat-in-a-dogs-world;", name: "cat in a dogs world", code: "MEW", marketCapRank: 150, coinGeckoId: "cat-in-a-dogs-world;")
                
        newCoins.append(contentsOf: [xdcCoin, mewCoin])
        
        return newCoins
    }
    
    func newBlockchains() -> [BlockchainRecord] {
        var blockchainRecords = [BlockchainRecord]()
        let xdcBlockchain = BlockchainRecord(uid: "xdc-network", name: "xdc-network")
        blockchainRecords.append(xdcBlockchain)
        
        return blockchainRecords
    }
    
    func newTokens() -> [TokenRecord] {
        var tokenRecords = [TokenRecord]()
        let xdcToken = TokenRecord(coinUid: "xdce-crowd-sale", blockchainUid: "xdc-network", type: "native", decimals: 18)
        let mewToken = TokenRecord(coinUid: "cat-in-a-dogs-world", blockchainUid: "solana", type: "spl", decimals: 5, reference: "MEW1gQWJ3nEXg2qgERiKu7FAFj79PHvQVREQUzScPP5")
        
        tokenRecords.append(contentsOf: [xdcToken, mewToken])
        
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
