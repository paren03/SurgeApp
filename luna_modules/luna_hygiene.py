"""Source-code hygiene rules and AST visitor.

Extracted from ``worker.py`` (step 5 of modularity refactor).
``verify_code_hygiene`` stays in ``worker.py`` for now (it is part of
the verification pipeline extracted in step 6).
"""

from __future__ import annotations

import ast
from pathlib import Path
from typing import List, Optional

from luna_modules.luna_paths import LEGACY_HYGIENE_WHITELIST_BY_FILE

HYGIENE_BANNED_NAME_FRAGMENTS = (
    "_replacement",
    "_guided_",
    "_source",
    "_apply_source",
    "_engine_source",
    "_template",
    "_patch_text",
)
HYGIENE_ASSIGN_BANNED_FRAGMENTS = (
    "_source",
    "_apply_source",
    "_engine_source",
    "_template",
    "_patch_text",
    "_replacement",
)
HYGIENE_IDENTIFIER_SUFFIX_BLOCKLIST = ("_patch_text", "_template", "_source")
HYGIENE_NESTED_FUNCTION_MAX_LINES = 50
HYGIENE_LOCAL_STRING_ASSIGN_MAX_LINES = 15

LEGACY_HYGIENE_WHITELIST = {"is_guided_improvement_command", "run_guided_self_improvement"}


def _hygiene_extract_target_names(target: ast.AST) -> List[str]:
    if isinstance(target, ast.Name):
        return [str(target.id)]
    if isinstance(target, ast.Attribute):
        return [str(target.attr)]
    if isinstance(target, (ast.Tuple, ast.List)):
        names: List[str] = []
        for item in target.elts:
            names.extend(_hygiene_extract_target_names(item))
        return names
    return []


def _hygiene_string_literal_line_count(node: ast.AST) -> int:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return len(str(node.value).splitlines())
    if isinstance(node, ast.JoinedStr):
        pieces: List[str] = []
        dynamic = False
        for value in node.values:
            if isinstance(value, ast.Constant) and isinstance(value.value, str):
                pieces.append(str(value.value))
            else:
                dynamic = True
        if pieces and not dynamic:
            return len("".join(pieces).splitlines())
    return 0


def _hygiene_forbidden_fragment(name: str) -> Optional[str]:
    lowered = str(name).lower()
    for fragment in HYGIENE_BANNED_NAME_FRAGMENTS:
        if fragment in lowered:
            return fragment
    return None


def _hygiene_forbidden_suffix(name: str) -> Optional[str]:
    lowered = str(name).lower()
    for suffix in HYGIENE_IDENTIFIER_SUFFIX_BLOCKLIST:
        if lowered.endswith(suffix):
            return suffix
    return None


def _hygiene_check_named_node(name: str, kind: str, violations: List[str], source_path: str = "") -> None:
    source_name = Path(str(source_path or "")).name
    fragment = _hygiene_forbidden_fragment(name)
    if fragment:
        whitelist = set(LEGACY_HYGIENE_WHITELIST_BY_FILE.get(source_name, set())) | set(LEGACY_HYGIENE_WHITELIST)
        legacy_fragment = "_gui" + "ded_"
        permit_legacy_name = (
            fragment == legacy_fragment
            and kind == "function"
            and str(name) in whitelist
            and source_name in LEGACY_HYGIENE_WHITELIST_BY_FILE
        )
        if not permit_legacy_name:
            violations.append(
                f"forbidden_identifier_detected :: {kind} '{name}' contains forbidden fragment '{fragment}'"
            )
            return
    suffix = _hygiene_forbidden_suffix(name)
    if suffix:
        violations.append(
            f"forbidden_identifier_detected :: {kind} '{name}' ends with forbidden suffix '{suffix}'"
        )


def _hygiene_check_nested_size(
    node: ast.AST,
    name: str,
    function_stack: List[ast.AST],
    violations: List[str],
) -> None:
    if not function_stack:
        return
    start_line = int(getattr(node, "lineno", 0))
    end_line = int(getattr(node, "end_lineno", start_line))
    line_count = end_line - start_line + 1
    if line_count > HYGIENE_NESTED_FUNCTION_MAX_LINES:
        parent = function_stack[-1]
        parent_name = getattr(parent, "name", "<unknown>")
        violations.append(
            f"nested_wrapper_detected :: nested function '{name}' inside '{parent_name}' "
            f"exceeds {HYGIENE_NESTED_FUNCTION_MAX_LINES} lines ({line_count})"
        )


def _hygiene_check_assignment(
    targets: List[str],
    value: Optional[ast.AST],
    violations: List[str],
) -> None:
    for target_name in targets:
        fragment = _hygiene_forbidden_fragment(target_name)
        if fragment:
            violations.append(
                f"string_template_detected :: assignment target '{target_name}' "
                f"contains forbidden fragment '{fragment}'"
            )
            continue
        suffix = _hygiene_forbidden_suffix(target_name)
        if suffix:
            violations.append(
                f"string_template_detected :: assignment target '{target_name}' "
                f"ends with forbidden suffix '{suffix}'"
            )
    if value is None or not targets:
        return
    line_count = _hygiene_string_literal_line_count(value)
    if line_count > HYGIENE_LOCAL_STRING_ASSIGN_MAX_LINES:
        violations.append(
            f"string_template_detected :: assignment '{targets[0]}' stores multiline string of "
            f"{line_count} lines (> {HYGIENE_LOCAL_STRING_ASSIGN_MAX_LINES})"
        )


class HygieneVisitor(ast.NodeVisitor):
    def __init__(self, source_path: str = "") -> None:
        self.source_path = str(source_path or "")
        self.function_stack: List[ast.AST] = []
        self.violations: List[str] = []

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        _hygiene_check_named_node(str(node.name), "function", self.violations, self.source_path)
        _hygiene_check_nested_size(node, str(node.name), self.function_stack, self.violations)
        self.function_stack.append(node)
        self.generic_visit(node)
        self.function_stack.pop()

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        _hygiene_check_named_node(str(node.name), "function", self.violations, self.source_path)
        _hygiene_check_nested_size(node, str(node.name), self.function_stack, self.violations)
        self.function_stack.append(node)
        self.generic_visit(node)
        self.function_stack.pop()

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        _hygiene_check_named_node(str(node.name), "class", self.violations, self.source_path)
        self.generic_visit(node)

    def visit_Assign(self, node: ast.Assign) -> None:
        targets: List[str] = []
        for target in node.targets:
            targets.extend(_hygiene_extract_target_names(target))
        _hygiene_check_assignment(targets, node.value, self.violations)
        self.generic_visit(node)

    def visit_AnnAssign(self, node: ast.AnnAssign) -> None:
        targets = _hygiene_extract_target_names(node.target)
        _hygiene_check_assignment(targets, node.value, self.violations)
        self.generic_visit(node)
