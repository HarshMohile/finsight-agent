# test_setup.py
# -------------
# Smoke test — run this manually: uv run python test_setup.py
# Not a pytest file. No assertions. Just clear pass/fail output.
# Run this every time you set up on a new machine or after major changes.
# Author : Harsh Mohile
import os
import sys

from dotenv import load_dotenv

# Load .env file before reading any keys
load_dotenv()


def check_env_keys():
    """Check 1: Are all required API keys present in .env?"""
    print("\n--- Check 1: Environment Keys ---")

    required_keys = {
        "GROQ_API_KEY": "Get from console.groq.com",
        "LANGSMITH_API_KEY": "Get from smith.langchain.com",
        "LANGSMITH_PROJECT": "Should be: finsight-agent",
    }

    all_present = True
    for key, hint in required_keys.items():
        value = os.getenv(key)
        if value:
            # Show first 8 chars only — confirms it's loaded without exposing the key
            preview = value[:8] + "..." if len(value) > 8 else value
            print(f"  ✅ {key}: {preview}")
        else:
            print(f"  ❌ {key} — Missing. Hint: {hint}")
            all_present = False

    return all_present


def check_llm_connection():
    """Check 2: Can we reach Groq and get a real response?"""
    print("\n--- Check 2: LLM Connection (Groq) ---")

    try:
        from langchain_core.messages import HumanMessage
        from langchain_groq import ChatGroq

        llm = ChatGroq(
            model="llama-3.3-70b-versatile",
            temperature=0,
            max_tokens=10,  # tiny limit — we just need one word back
        )

        response = llm.invoke([HumanMessage(content="Reply with the single word: CONNECTED")])

        if "CONNECTED" in response.content.upper():
            print(f"  ✅ Groq responded: {response.content.strip()}")
            return True
        else:
            print(f"  ⚠️  Groq responded but unexpectedly: {response.content.strip()}")
            return True  # connection works, model just being creative

    except Exception as e:
        print(f"  ❌ Groq connection failed: {e}")
        return False


def check_project_imports():
    """Check 3: Can Python find our own modules?"""
    print("\n--- Check 3: Project Structure ---")

    checks = [
        ("schemas.models", "InvoiceExtraction"),
        ("agents.invoice_agent", "run_invoice_agent"),
    ]

    all_ok = True
    for module, attribute in checks:
        try:
            imported = __import__(module, fromlist=[attribute])
            getattr(imported, attribute)
            print(f"  ✅ {module}.{attribute} — importable")
        except ModuleNotFoundError:
            print(f"  ⏭️  {module} — not created yet (expected at this stage)")
        except Exception as e:
            print(f"  ❌ {module} — error: {e}")
            all_ok = False

    return all_ok


def main():
    print("=" * 50)
    print("  FinSight Agent — Environment Smoke Test")
    print("=" * 50)

    results = {
        "env_keys": check_env_keys(),
        "llm_connection": check_llm_connection(),
        "project_imports": check_project_imports(),
    }

    print("\n--- Summary ---")
    for check, passed in results.items():
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"  {status}  {check}")

    if all(results.values()):
        print("\n✅ All checks passed.")
        print("   Next: uv run uvicorn api.main:app --reload")
        print("   Traces: https://smith.langchain.com\n")
    else:
        print("\n⚠️  Some checks failed — fix above before writing agent code.\n")
        sys.exit(1)  # exits with error code so CI/CD knows it failed


if __name__ == "__main__":
    main()
