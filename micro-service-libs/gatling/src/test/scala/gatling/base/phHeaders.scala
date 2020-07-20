package gatling.base

case class phToken(token: String, tokenType: String = "Bearer")

object phHeaders {
    val headers_base = Map(
        "Accept" -> "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
        "Upgrade-Insecure-Requests" -> "1")

    val headers_json = Map(
        "Content-Type" -> "application/json,charset=utf-8"
    )

    def headers_json_token(implicit token: phToken): Map[String,String] = Map(
        "Content-Type" -> "application/json,charset=utf-8",
        "Authorization" -> (token.tokenType + " " + token.token)
    )
}
