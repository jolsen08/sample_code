package com.jimmy.demo;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * Service that calls the Gemini API to pull out the total price from invoice text.
 * This was mostly vibe-coded while I was learning Java Spring Boot.
 *
 * <p>It sends the raw invoice text to the Gemini {@code generateContent} endpoint
 * with a simple prompt and then walks the JSON response to grab just the final
 * invoice total. If we can't find a price in the response, it falls back to the
 * literal string {@code "No price found"}.</p>
 */
@Service
public class GeminiInvoiceService {

  private static final String MODEL_ENDPOINT = "/v1beta/models/gemini-2.5-flash:generateContent";
  private final WebClient webClient;
  private final ObjectMapper objectMapper;
  private final String apiKey;

  /**
   * Build a new {@code GeminiInvoiceService}.
   *
   * @param apiKey           Gemini API key used for authentication
   * @param webClientBuilder Spring WebClient builder that gives us a configured client
   * @param objectMapper     Jackson ObjectMapper used to parse Gemini responses
   */
  public GeminiInvoiceService(
      @Value("{api_key}") String apiKey,
      WebClient.Builder webClientBuilder,
      ObjectMapper objectMapper
  ) {
    // Stash the API key and ObjectMapper so we can reuse them later.
    // NOTE: In real code, don't hardcode the API key in @Value – pull it from
    // config (application.yml, env vars, secret store, etc.).
    this.apiKey = apiKey;
    this.objectMapper = objectMapper;
    this.webClient = webClientBuilder
        .baseUrl("https://generativelanguage.googleapis.com")
        .build();
  }

  /**
   * Ask Gemini for the total invoice price given the raw invoice text.
   *
   * <p>We build a simple natural language prompt, send it to the configured Gemini
   * model endpoint, and then peel the answer back out of the JSON response.</p>
   *
   * @param invoiceText raw text content of the invoice
   * @return the total price as returned by Gemini, or {@code "No price found"} when
   *         the model doesn't give us anything usable
   * @throws IllegalStateException if the API key is missing or the call fails
   */
  public String findPrice(String invoiceText) {
    // Make sure we actually have an API key configured; bail out early if we don't.
    if (apiKey == null || apiKey.isBlank()) {
      throw new IllegalStateException("Gemini API key is not configured (set property 'gemini.api.key').");
    }

    // Build a prompt that tells Gemini to only send back the total invoice price.
    String prompt = """
        You are an expert invoice analyst.
        Read the invoice text provided below and return ONLY the total invoice price (including currency symbol if present).
        If no price exists, respond exactly with the phrase "No price found".

        Invoice text:
        %s
        """.formatted(invoiceText);

    // Shape the JSON payload the way the Gemini API expects it
    // (contents -> role + parts[text]).
    Map<String, Object> payload = Map.of(
        "contents", List.of(
            Map.of(
                "role", "user",
                "parts", List.of(Map.of("text", prompt))
            )
        )
    );

    try {
      // Fire off the POST request to Gemini using WebClient.
      // Grab the response body as a raw JSON string.
      String rawResponse = webClient.post()
          .uri(uriBuilder -> uriBuilder
              .path(MODEL_ENDPOINT)
              .queryParam("key", apiKey)
              .build())
          .contentType(MediaType.APPLICATION_JSON)
          .bodyValue(payload)
          .retrieve()
          .bodyToMono(String.class)
          .timeout(Duration.ofSeconds(30))
          .onErrorResume(WebClientResponseException.class, ex -> Mono.error(
              new IllegalStateException("Gemini API error: " + ex.getStatusCode() + " " + ex.getResponseBodyAsString(), ex)))
          .block();

      // Parse the JSON response and extract the price text.
      return parsePrice(rawResponse);
    } catch (IllegalStateException e) {
      throw e;
    } catch (Exception e) {
      // Wrap anything else in a generic IllegalStateException so callers always
      // see one consistent error type here.
      throw new IllegalStateException("Failed to contact Gemini: " + e.getMessage(), e);
    }
  }

  /**
   * Take the Gemini JSON response and pull out the first non-blank text segment.
   *
   * <p>The {@code generateContent} endpoint gives us a list of candidates, each with
   * a {@code content.parts} list. We walk that structure and return the first
   * non-empty {@code text} value we find. If we never find one, we fall back to
   * {@code "No price found"}.</p>
   *
   * @param rawJson raw JSON string returned by Gemini
   * @return the extracted text content, or {@code "No price found"} if nothing usable exists
   * @throws Exception if the JSON cannot be parsed
   */
  private String parsePrice(String rawJson) throws Exception {
    // Turn the raw JSON into a tree and jump straight to the `candidates` array.
    JsonNode root = objectMapper.readTree(rawJson);

    // No candidates means we effectively didn't get a price back.
    JsonNode candidates = root.path("candidates");
    if (!candidates.isArray() || candidates.isEmpty()) {
      return "No price found";
    }

    // Walk through all candidates and their parts to grab the first text snippet.
    for (JsonNode candidate : candidates) {
      JsonNode parts = candidate.path("content").path("parts");
      if (!parts.isArray()) {
        continue;
      }
      for (JsonNode part : parts) {
        JsonNode textNode = part.get("text");
        if (textNode != null && !textNode.asText().isBlank()) {
          String responseText = textNode.asText().trim();
          // As soon as we see non-blank text, return it; otherwise say "No price found".
          return responseText.isEmpty() ? "No price found" : responseText;
        }
      }
    }
    // If we got this far, nothing worked out, so fall back to the default.
    return "No price found";
  }
}
