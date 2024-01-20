package io.orderflow.order.domain;

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * The state machine is the cheapest place to catch a saga design mistake, so it is tested
 * exhaustively: every legal edge, every terminal state, and the specific illegal transitions that
 * a badly ordered event stream would attempt.
 */
class OrderStateMachineTest {

    private static final String ORDER_ID = "11111111-2222-3333-4444-555555555555";

    private final OrderStateMachine stateMachine = new OrderStateMachine();

    @ParameterizedTest(name = "{0} -> {1} is allowed")
    @CsvSource({
            "PENDING,INVENTORY_RESERVED",
            "PENDING,REJECTED",
            "PENDING,CANCELLED",
            "PENDING,FAILED",
            "INVENTORY_RESERVED,PAYMENT_CAPTURED",
            "INVENTORY_RESERVED,REJECTED",
            "INVENTORY_RESERVED,CANCELLED",
            "INVENTORY_RESERVED,FAILED",
            "PAYMENT_CAPTURED,CONFIRMED",
            "PAYMENT_CAPTURED,FAILED"
    })
    @DisplayName("legal transitions are accepted")
    void allowsLegalTransitions(OrderStatus from, OrderStatus to) {
        assertThat(stateMachine.canTransition(from, to)).isTrue();
        assertThat(stateMachine.transition(ORDER_ID, from, to)).isEqualTo(to);
    }

    @ParameterizedTest(name = "{0} -> {1} is rejected")
    @CsvSource({
            // Payment cannot be captured before stock is held: the saga order would be violated.
            "PENDING,PAYMENT_CAPTURED",
            "PENDING,CONFIRMED",
            // An order cannot be confirmed straight from a stock hold; payment must land first.
            "INVENTORY_RESERVED,CONFIRMED",
            // Late compensation after a capture must not silently cancel a confirmed order.
            "PAYMENT_CAPTURED,CANCELLED",
            "PAYMENT_CAPTURED,REJECTED",
            "PAYMENT_CAPTURED,INVENTORY_RESERVED"
    })
    @DisplayName("illegal transitions raise IllegalStateTransitionException")
    void rejectsIllegalTransitions(OrderStatus from, OrderStatus to) {
        assertThat(stateMachine.canTransition(from, to)).isFalse();
        assertThatThrownBy(() -> stateMachine.transition(ORDER_ID, from, to))
                .isInstanceOf(IllegalStateTransitionException.class)
                .hasMessageContaining(ORDER_ID)
                .hasMessageContaining(from.name())
                .hasMessageContaining(to.name());
    }

    @ParameterizedTest
    @EnumSource(value = OrderStatus.class, names = {"CONFIRMED", "REJECTED", "CANCELLED", "FAILED"})
    @DisplayName("terminal states have no outgoing transitions")
    void terminalStatesAreFinal(OrderStatus terminal) {
        assertThat(terminal.isTerminal()).isTrue();
        assertThat(stateMachine.allowedFrom(terminal)).isEmpty();

        List<OrderStatus> everyStatus = Arrays.asList(OrderStatus.values());
        assertThat(everyStatus)
                .filteredOn(target -> stateMachine.canTransition(terminal, target))
                .isEmpty();
    }

    @Test
    @DisplayName("a duplicate delivery that re-applies the current state is a no-op, not an error")
    void reapplyingTheSameStatusIsANoOp() {
        assertThat(stateMachine.transitionIfNeeded(
                ORDER_ID, OrderStatus.INVENTORY_RESERVED, OrderStatus.INVENTORY_RESERVED)).isFalse();

        assertThatCode(() -> stateMachine.transitionIfNeeded(
                ORDER_ID, OrderStatus.CONFIRMED, OrderStatus.CONFIRMED))
                .doesNotThrowAnyException();
    }

    @Test
    @DisplayName("transitionIfNeeded still enforces the graph for genuine changes")
    void transitionIfNeededStillValidates() {
        assertThat(stateMachine.transitionIfNeeded(
                ORDER_ID, OrderStatus.PENDING, OrderStatus.INVENTORY_RESERVED)).isTrue();

        assertThatThrownBy(() -> stateMachine.transitionIfNeeded(
                ORDER_ID, OrderStatus.CONFIRMED, OrderStatus.CANCELLED))
                .isInstanceOf(IllegalStateTransitionException.class);
    }

    @Test
    @DisplayName("only the states that actually hold stock report holdsInventory")
    void onlyReservedStatesHoldInventory() {
        assertThat(OrderStatus.INVENTORY_RESERVED.holdsInventory()).isTrue();
        assertThat(OrderStatus.PAYMENT_CAPTURED.holdsInventory()).isTrue();
        assertThat(OrderStatus.PENDING.holdsInventory()).isFalse();
        assertThat(OrderStatus.CANCELLED.holdsInventory()).isFalse();
    }

    @Test
    @DisplayName("null states are never transitionable")
    void nullStatesAreRejected() {
        assertThat(stateMachine.canTransition(null, OrderStatus.PENDING)).isFalse();
        assertThat(stateMachine.canTransition(OrderStatus.PENDING, null)).isFalse();
    }
}
