package simpledb.storage;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import simpledb.transaction.TransactionId;

public class WaitForGraph {
    private final Map<TransactionId, Map<TransactionId, Integer>> graph = new ConcurrentHashMap<>();

    public void setWait(TransactionId from, TransactionId to, int count) {
        if (from.equals(to))
            return;
        if (count <= 0) {
            Map<TransactionId, Integer> edges = graph.get(from);
            if (edges != null) {
                edges.remove(to);
                if (edges.isEmpty())
                    graph.remove(from);
            }
        } else {
            graph.computeIfAbsent(from, k -> new ConcurrentHashMap<>()).put(to, count);
        }
    }

    public void incrementWait(TransactionId from, TransactionId to) {
        if (from.equals(to))
            return;
        graph.compute(from, (k, v) -> {
            if (v == null) {
                Map<TransactionId, Integer> result = new ConcurrentHashMap<>();
                result.put(to, 1);
                return result;
            }
            v.compute(to, (kk, vv) -> {
                return vv + 1;
            });
            return v;
        });
    }

    public void decrementWait(TransactionId from, TransactionId to) {
        if (from.equals(to))
            return;
        graph.compute(from, (k, v) -> {
            if (v == null)
                return null;
            v.compute(to, (kk, vv) -> {
                if (vv == null)
                    return null;
                return vv - 1;
            });
            return v;
        });
    }

    public int getWait(TransactionId from, TransactionId to) {
        AtomicInteger atomicInteger = new AtomicInteger(0);
        graph.computeIfPresent(to, (k, v) -> {
            if (v == null)
                return null;
            atomicInteger.set(v.getOrDefault(to, 0));
            return v;
        });
        return atomicInteger.get();
    }

    public void removeNode(TransactionId node) {
        // Remove all outgoing edges from this node
        graph.remove(node);

        // Remove all incoming edges to this node
        for (Map<TransactionId, Integer> edges : graph.values())
            edges.remove(node);
    }

    public boolean hasCycle() {
        Set<TransactionId> visited = new HashSet<>();
        Set<TransactionId> recStack = new HashSet<>();

        for (TransactionId node : graph.keySet())
            if (dfsHasCycle(node, visited, recStack))
                return true;

        return false;
    }

    private boolean dfsHasCycle(TransactionId node, Set<TransactionId> visited, Set<TransactionId> recStack) {
        // Base cases
        if (recStack.contains(node))
            return true;
        if (visited.contains(node))
            return false;

        visited.add(node);
        recStack.add(node);

        for (TransactionId neighbor : graph.getOrDefault(node, Map.of()).keySet())
            if (dfsHasCycle(neighbor, visited, recStack))
                return true;

        recStack.remove(node);
        return false;
    }
}