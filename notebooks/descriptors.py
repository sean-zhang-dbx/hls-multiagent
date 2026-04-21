"""RDKit ECFP molecular fingerprint helpers (adapted from AiChemy)."""

import numpy as np
from rdkit.Chem import AllChem, MolFromSmiles

fpgen = AllChem.GetMorganGenerator(radius=2, fpSize=1024)


def get_ecfp(mol, radius: int = 2, fpSize: int = 1024) -> np.ndarray:
    """Compute ECFP for an RDKit Mol object."""
    gen = AllChem.GetMorganGenerator(radius=radius, fpSize=fpSize)
    return gen.GetFingerprintAsNumPy(mol)


def smiles_to_ecfp(smiles: str, generator=None) -> np.ndarray:
    """Convert a SMILES string to an ECFP4 fingerprint array."""
    gen = generator or fpgen
    mol = MolFromSmiles(smiles)
    if mol is None:
        raise ValueError(f"Invalid SMILES: {smiles}")
    return gen.GetFingerprintAsNumPy(mol)


def smiles_to_bitstring(smiles: str) -> str:
    """Convert a SMILES string to a 1024-char bitstring."""
    fp = smiles_to_ecfp(smiles)
    return "".join(str(int(x)) for x in fp)
