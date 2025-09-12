import Foundation

class DTProvider {
    private static let baseURL: String = "https://admin.dextrade.com/api/public/"

    static func remoteCoins() async throws -> DTCoinsResponse {
        let urlPath = Self.baseURL + "coins"
        
        guard let url = URL(string: urlPath) else {
            throw NSError(domain: "Invalid URL", code: -1)
        }
        
        var request = URLRequest(url: url)
        request.httpMethod = "GET"
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        
        do {
            let (data, response) = try await URLSession.shared.data(for: request)
            let dataResponse = String(decoding: data, as: UTF8.self)
            print(dataResponse)

            return try JSONDecoder().decode(DTCoinsResponse.self, from: data)
        } catch {
            throw error
        }
    }
    
    static func remoteTokens() async throws -> DTTokensResponse {
        let urlPath = Self.baseURL + "tokens"
        
        guard let url = URL(string: urlPath) else {
            throw NSError(domain: "Invalid URL", code: -1)
        }
        
        var request = URLRequest(url: url)
        request.httpMethod = "GET"
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        
        do {
            let (data, response) = try await URLSession.shared.data(for: request)
            let dataResponse = String(decoding: data, as: UTF8.self)
            print(dataResponse)

            return try JSONDecoder().decode(DTTokensResponse.self, from: data)
        } catch {
            throw error
        }
    }
}
