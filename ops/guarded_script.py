"""Keep the guards of generated helper scripts active under ``python -O``.

The installer and updater ship Python source to ``python -c`` inside a
container. ``assert`` statements in that source vanish under ``-O`` or
``PYTHONOPTIMIZE``; this rewrites each one into an explicit raise first.
"""
import ast


def explicit_guard_source(source):
    """Return ``source`` with every ``assert`` replaced by ``if not ...: raise AssertionError``."""
    class ReplaceAssertions(ast.NodeTransformer):
        def visit_Assert(self, node):
            return ast.copy_location(ast.If(test=ast.UnaryOp(op=ast.Not(), operand=self.visit(node.test)),
                body=[ast.Raise(exc=ast.Call(func=ast.Name(id='AssertionError', ctx=ast.Load()),
                    args=[] if node.msg is None else [self.visit(node.msg)], keywords=[]), cause=None)],
                orelse=[]), node)
    tree = ast.fix_missing_locations(ReplaceAssertions().visit(ast.parse(source)))
    return ast.unparse(tree)
