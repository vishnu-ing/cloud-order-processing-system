package io.orderflow.common.observability;

import java.io.IOException;

import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

/**
 * Servlet filter that seeds the MDC for every inbound HTTP request and echoes the correlation id
 * back on the response, so a client can quote it in a support ticket.
 *
 * <p>Registered explicitly by each MVC service (never component-scanned) so the reactive
 * api-gateway — which has its own {@code WebFilter} equivalent — is unaffected.
 */
public class CorrelationIdFilter implements Filter {

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        String correlationId = null;
        if (request instanceof HttpServletRequest httpRequest) {
            correlationId = httpRequest.getHeader(CorrelationContext.CORRELATION_ID_HEADER);
        }
        correlationId = CorrelationContext.orNew(correlationId);

        try {
            CorrelationContext.put(correlationId, null, null);
            if (response instanceof HttpServletResponse httpResponse) {
                httpResponse.setHeader(CorrelationContext.CORRELATION_ID_HEADER, correlationId);
            }
            chain.doFilter(request, response);
        } finally {
            CorrelationContext.clear();
        }
    }
}
